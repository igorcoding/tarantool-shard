local digest = require('digest')
local fiber = require('fiber')
local log = require('log')
local obj = require('obj')
local yaml = require('yaml')

local connpool = require('shardconnpool')
local shardutils = require('shardutils')
local shardschema = require('schema')


local raftshard = {
	configured = false,
	utils = shardutils,
}
local self = raftshard


function raftshard.configure(cfg)
	local schema_cfg = cfg.schema
	cfg.schema = nil
	
	self.schema = shardschema(schema_cfg)
	self.pool = connpool(cfg, self.schema)
	
	self.configured = true
end

function raftshard.connect()
	self.check_configured()
	self.pool:connect()
end


function raftshard.check_configured()
	if not self.configured then
		self.utils.error('raftShard is not configured. Please call configure() first.')
	end
end

function raftshard.check_space(space)
	if box.space[space] == nil then
		self.utils.error('Space %s does not exist', space)
	end
end

function raftshard.check_space_and_index(space, index)
	if box.space[space] == nil then
		self.utils.error('Space %s does not exist', space)
	end
	
	if box.space[space].index[index] == nil then
		self.utils.error('Index %s of space %s does not exist', index, space)
	end
end

function raftshard.check_ttl(shard_id, ttl)
	if ttl <= 0 then
		self.utils.error("Can't redirect request to shard %d (ttl=0)", shard_id)
	end
end


function raftshard.mynode()
	return self.pool:get_by_uuid(box.info.server.uuid)
end

function raftshard.myshard()
	local node = self.mynode()
	if node == nil then
		log.error('raftshard.myshard returned nil')
		return nil
	end
	return node.group_id
end


function raftshard.myconn()
	local node = self.mynode()
	if node == nil then
		log.error('raftshard.myconn returned nil')
		return nil
	end
	return node.conn
end


function raftshard.myraft()
	local node = self.mynode()
	if node == nil then
		log.error('raftshard.myraft returned nil')
		return nil
	end
	return node.raft
end


local function __select(conn, space, index, key, opts)
	return conn.space[space].index[index]:select(key, opts)
end

local function __get(conn, space, index, key)
	return conn.space[space].index[index]:get(key)
end

local function __count(conn, space, index, key, opts)
	return conn.space[space].index[index]:count(key, opts)
end

local function __insert(conn, space, tuple)
	return conn.space[space]:insert(tuple)
end

local function __replace(conn, space, tuple)
	return conn.space[space]:replace(tuple)
end

local function __delete(conn, space, index, key)
	return conn.space[space].index[index]:delete(key)
end


function raftshard._select_me(space, index, key, opts)
	-- print('Got _select_me request from', box.session.peer())
	local tuples = __select(box, space, index, key, opts)
	if tuples and #tuples ~= 0 then return tuples end
	
	if not self.pool:curr_is_me_leader() then
		local node = self.pool:curr_get_my_leader()
		if node then
			tuples = __select(node.conn, space, index, key, opts)
		end
		
		if tuples and #tuples ~= 0 then return tuples end
	end
	
	return tuples
end
function raftshard._select_me_unpack(...) return unpack(self._select_me(...)) end

function raftshard._get_me(space, index, key)
	-- print('Got _get_me request from', box.session.peer())
	local tuple = __get(box, space, index, key)
	if tuple then return tuple end
		
	if not self.pool:curr_is_me_leader() then
		local node = self.pool:curr_get_my_leader()
		if node == nil then
			tuple = __get(node.conn, space, index, key)
		end
		
		if tuple then return tuple end
	end
	
	return tuple
end

function raftshard._count_me(space, index, key, opts)
	-- print('Got _count_me request from', box.session.peer())
	local count = __count(box, space, index, key)
	if count and count > 0 then return count end
		
	if not self.pool:curr_is_me_leader() then
		local node = self.pool:curr_get_my_leader()
		if node == nil then
			count = __count(node.conn, space, index, key)
		end
		
		if count and count > 0 then return count end
	end
	
	return 0
end

function raftshard._delete_me(space, index, key)
	return __delete(box, space, index, key)
end



function raftshard.select(space, index, key, opts)
	-- box.shard.select('tokens', 'primary', {'token-id-1'})
	self.check_space_and_index(space, index)
	
	local shard_id = self.schema:curr(space, index, key)
	
	if self.pool:curr_is_me_by(shard_id) then
		local tuples = self._select_me(space, index, key, opts)
		if tuples and #tuples > 0 then return tuples end
		
	elseif shard_id == self.schema.ALL_SHARDS then
		local results = self.execute_on_many_shards('box.shard._select_me_unpack', space, index, key, opts)
		if results == nil then
			self.utils.error('Timeout exceeded')
		end
		local filtered_results = {}
		for uuid,response in pairs(results) do
			local success, resp = unpack(response)
			if not success then
				self.utils.error(resp)
			else
				filtered_results[uuid] = resp
			end
		end
		return self.utils.merge_tuples(filtered_results, space, index, key, opts)
	else
		-- not my shard. we need to make request to another shard's leader
		local node = self.pool:curr_leader_by(shard_id) or self.pool:curr_by(shard_id)
		if node then
			local tuples = __select(node.conn, space, index, key, opts)
			if tuples and #tuples > 0 then return tuples end
		else
			self.utils.error('Shard #%d is completely offline', shard_id)
		end
	end
	
	return box.tuple.new({})
end

function raftshard.get(space, index, key)
	self.check_space_and_index(space, index)
	
	local shard_id = self.schema:curr(space, index, key)
	
	if self.pool:curr_is_me_by(shard_id) then
		local tuple = self._get_me(space, index, key)
		if tuple then return tuple end
		
	elseif shard_id == self.schema.ALL_SHARDS then
		local results = self.execute_on_many_shards('box.shard._get_me', space, index, key)
		if results == nil then
			self.utils.error('Timeout exceeded')
		end
		
		local tuple = nil
		for _,response in pairs(results) do
			local success, resp = unpack(response)
			if not success then
				self.utils.error(resp)
			else
				if tuple == nil then
					tuple = resp
				else
					box.error(box.error.MORE_THAN_ONE_TUPLE)
				end
			end
		end
		if tuple then return tuple end
	else
		-- not my shard. we need to make request to another shard's leader
		local node = self.pool:curr_leader_by(shard_id) or self.pool:curr_by(shard_id)
		if node then
			local tuple = __get(node.conn, space, index, key)
			if tuple then return tuple end
		else
			self.utils.error('Shard #%d is completely offline', shard_id)
		end
	end
	
	return nil
end

function raftshard.count(space, index, key, opts)
	self.check_space_and_index(space, index)
	
	if type(key) ~= 'table' then key = {key} end
	
	local shard_id = self.schema:curr(space, index, key)
	
	if self.pool:curr_is_me_by(shard_id) then
		local count = self._count_me(space, index, key, opts)
		if count and count > 0 then return count end
	elseif shard_id == self.schema.ALL_SHARDS then
		local results = self.execute_on_many_shards('box.shard._count_me', space, index, key, opts)
		local total_count = 0
		for _,response in pairs(results) do
			local success, resp = unpack(response)
			if not success then
				self.utils.error(resp)
			else
				local count = resp[1][1]
				if count ~= nil then
					total_count = total_count + count
				end
			end
		end
		if total_count > 0 then return total_count end
	else
		-- not my shard. we need to make request to another shard's leader
		local node = self.pool:curr_leader_by(shard_id) or self.pool:curr_by(shard_id)
		if node then
			local count = __count(node.conn, space, index, key, opts)
			if count and count > 0 then return count end
		else
			self.utils.error('Shard #%d is completely offline', shard_id)
		end
	end
	
	return 0
end


function raftshard.insert(space, index, tuple) return self._insert(1, space, index, tuple) end
function raftshard._insert(ttl, space, index, tuple)
	if index ~= 'primary' then
		self.utils.error('Insert to not primary index is not supported')
	end
	self.check_space(space)
	
	ttl = tonumber(ttl)
	
	
	local shard_id = self.schema:curr(space, index, tuple)
	
	if self.pool:curr_is_me_leader_by(shard_id) then
		return __insert(box, space, tuple)
	elseif shard_id == self.schema.ALL_SHARDS then
		self.utils.error('Cannot execute insert on all shards')
	else
		self.check_ttl(shard_id, ttl)
		local node = self.pool:curr_leader_by(shard_id)
		if node then
			return unpack(node.conn:call('box.shard._insert', ttl - 1, space, index, tuple))
		end
		self.utils.error('Shard #%d leader is not accessible', shard_id)
	end
	
	return unpack({})
end


function raftshard.replace(space, index, tuple) return self._replace(1, space, index, tuple) end
function raftshard._replace(ttl, space, index, tuple)
	if index ~= 'primary' then
		self.utils.error('Replace to not primary index is not supported')
	end
	self.check_space(space)
	
	ttl = tonumber(ttl)
	
	local shard_id = self.schema:curr(space, index, tuple)
	
	if self.pool:curr_is_me_leader_by(shard_id) then
		return __replace(box, space, tuple)
	elseif shard_id == self.schema.ALL_SHARDS then
		self.utils.error('Cannot execute replace on all shards')
	else
		self.check_ttl(shard_id, ttl)
		local node = self.pool:curr_leader_by(shard_id)
		if node then
			return unpack(node.conn:call('box.shard._replace', ttl - 1, space, index, tuple))
		end
		self.utils.error('Shard #%d leader is not accessible', shard_id)
	end
	
	return unpack({})
end


function raftshard.delete(space, index, key) return self._delete(1, space, index, key) end
function raftshard._delete(ttl, space, index, key)
	self.check_space(space)
	
	ttl = tonumber(ttl)
	
	local shard_id = self.schema:curr(space, index, tuple)
	
	if self.pool:curr_is_me_by(shard_id) then
		return self._delete_me(space, index, key)
	elseif shard_id == self.schema.ALL_SHARDS then
		local results = self.execute_on_many_shards('box.shard._delete_me', space, index, key)
		for _, response in pairs(results) do
			local success, resp = unpack(response)
			if not success then
				self.utils.error(resp)
			end
		end
	else
		self.check_ttl(shard_id, ttl)
		local node = self.pool:curr_leader_by(shard_id)
		if node then
			return unpack(node.conn:call('box.shard._delete', ttl - 1, space, index, key))
		end
		self.utils.error('Shard #%d leader is not accessible', shard_id)
	end
	
	return unpack({})
end


function raftshard.execute_on_many_shards(method, space, index, ...)
	local nodes = {}
	for shard_id = 1, self.schema.curr_length do
		local node = self.pool:curr_by(shard_id)
		table.insert(nodes, node)
	end
	
	if #nodes == nil then
		return nil
	end
	
	local results = self.pool:call_nodes(nodes, method, space, index, ...)
	-- print(yaml.encode(results))
	return results
end


-- -------------------------------------------------------------

local function raftshard_generate_indexing(for_space, for_index)
	return {
		select = function(_, key, opts)
			return raftshard.select(for_space, for_index, key, opts)
		end,
		get = function(_, key)
			return raftshard.get(for_space, for_index, key)
		end,
		count = function(_, key, opts)
			return raftshard.count(for_space, for_index, key, opts)
		end,
		insert = function(_, tuple)
			return raftshard.insert(for_space, for_index, tuple)
		end,
		replace = function(_, tuple)
			return raftshard.replace(for_space, for_index, tuple)
		end,
		delete = function(_, key)
			return raftshard.delete(for_space, for_index, key)
		end,
	}
end

local function index_metatable(for_space, for_index_def, for_index_id)
	local for_index = for_index_def.name
	local obj = {}
	setmetatable(obj, {
		__index = raftshard_generate_indexing(for_space, for_index)
	})
	return obj
end


local function space_metatable(for_space)
	local generate_indexes_mt = function(for_space)
		local indexes = {}
		local space_indexes = box.space[for_space].index
		for index_name, index_def in pairs(space_indexes) do
			local index_def = {
				id = index_def.id,
				name = index_def.name
			}
			if indexes[index_def.id] == nil then
				indexes[index_def.id] = index_metatable(for_space, index_def)
				indexes[index_def.name] = indexes[index_def.id]
			end
		end
		return indexes
	end
	
	local obj = {
		index = generate_indexes_mt(for_space)
	}
	setmetatable(obj, {
		__index = raftshard_generate_indexing(for_space, 'primary')
	})
	return obj
end


local function create_raftshard_metatable()
	local generate_spaces_mt = function()
		local spaces = {}
		local real_spaces = box.space
		for space_name, space_def in pairs(real_spaces) do
			local space_def = {
				id = space_def.id,
				name = space_def.name
			}
			if spaces[space_def.id] == nil then
				spaces[space_def.id] = space_metatable(space_name)
				spaces[space_name] = spaces[space_def.id]
			end
		end
		return spaces
	end
	local spaces_mt = generate_spaces_mt()
	setmetatable(raftshard, {
		__index = function(self, key)
			if key == 'space' then
				return spaces_mt
			end
			return nil
		end
	})
	return raftshard
end


local box = require('box')
box.__oldshard__ = box.shard
raftshard = create_raftshard_metatable()
box.shard = raftshard
return raftshard
