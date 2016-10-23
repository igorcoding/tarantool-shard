local digest = require('digest')
local fiber = require('fiber')
local log = require('log')
local obj = require('obj')
local yaml = require('yaml')

local connpool = require('shardconnpool')
local shardutils = require('shardutils')
local shardschema = require('schema')
local resharding = require('resharding')


local raftshard = {
	configured = false,
	utils = shardutils,
}
local self = raftshard


function raftshard.configure(cfg)
	local schema_cfg = cfg.schema
	cfg.schema = nil
	
	self.shard_index = cfg.shard_index or 'primary'
	self.schema = shardschema(schema_cfg, self.shard_index)
	self.pool = connpool(cfg, self.schema)
	
	self.configured = true
	
	if self.schema.prev then
		self.resharding = resharding(self.pool, cfg.resharding)
	end
end

function raftshard.connect()
	self.check_configured()
	self.pool:connect()
end


function raftshard.check_configured()
	if not self.configured then
		self.utils.error('raftshard is not configured. Please call configure() first.')
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

function raftshard.info()
	local info = {
		id = box.info.server.id,
		uuid = box.info.server.uuid,
		srv = box.cfg.listen,
		state = self.schema.mode,
		
		status = 'unknown',
		conns = {},
		rafts = {},
	}
	
	for gid, raft in pairs(self.pool.rafts) do
		info.rafts[gid] = raft:info(false)
	end
	
	local lists = {
		curr = self.schema.curr_list,
		prev = self.schema.prev_list,
	}
	
	local online = {
		curr = { status = "unknown"; all = 0; connected = 0 };
		prev = { status = "unknown"; all = 0; connected = 0 };
	};
	
	local total = 0
	local total_connected = 0
	for schema, list in pairs(lists) do
		info.conns[schema] = {}
		
		for shard_id, shard in ipairs(list) do
			info.conns[schema][shard_id] = {}
			
			local leaders, others = {}, {}
			for _,node in ipairs(shard) do
				local peer = node.uri
				local node = self.pool:get_by_peer(peer)
				
				local to = node:is_leader() and leaders or others
				local node_status = self.pool:status(peer)
				total = total + 1
				online[schema].all = online[schema].all + 1
				if node_status == '+' then
					total_connected = total_connected + 1
					online[schema].connected = online[schema].connected + 1
				end
				
				table.insert(to, { peer; node_status })
			end
			
			if #leaders > 0 then
				info.conns[schema][shard_id]['leaders'] = leaders
			end
			if #others > 0 then
				info.conns[schema][shard_id]['others'] = others
			end
		end
		
		if online[schema].all == online[schema].connected then
			online[schema].status = "online"
		elseif online[schema].connected == 0 then
			online[schema].status = "offline"
		else
			online[schema].status = "partial"
		end
	end
	
	info.online = online
	info.total_peers = total
	info.total_connected = total_connected
	if total_connected == total then
		info.status = "online"
	elseif total_connected == 0 then
		info.status = "offline"
	else
		info.status = "partial"
	end
	
	
	return info
end

function raftshard.waitonline()
	return self.pool:ready()
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

local function __update(conn, space, index, key, upd)
	return conn.space[space].index[index]:update(key, upd)
end


function raftshard._select_me(schema, space, index, key, opts)
	-- print('Got _select_me request from', box.session.peer())
	local tuples = __select(box, space, index, key, opts)
	if tuples and #tuples ~= 0 then return tuples end
	
	local is_leader
	local get_leader
	if schema == 'curr' then
		is_leader = self.pool.curr_is_me_leader
		get_leader = self.pool.curr_get_my_leader
	elseif schema == 'prev' then
		is_leader = self.pool.prev_is_me_leader
		get_leader = self.pool.prev_get_my_leader
	else
		self.utils.error('Unknown schema name: %s. Valid values are: [curr, prev]', schema)
	end
	
	if not is_leader(self.pool) then
		local node = get_leader(self.pool)
		if node then
			tuples = __select(node.conn, space, index, key, opts)
		end
		
		if tuples and #tuples ~= 0 then return tuples end
	end
	
	return tuples
end
function raftshard._select_me_unpack(...) return unpack(self._select_me(...)) end

function raftshard._get_me(schema, space, index, key)
	-- print('Got _get_me request from', box.session.peer())
	local tuple = __get(box, space, index, key)
	if tuple then return tuple end
	
	local is_leader
	local get_leader
	if schema == 'curr' then
		is_leader = self.pool.curr_is_me_leader
		get_leader = self.pool.curr_get_my_leader
	elseif schema == 'prev' then
		is_leader = self.pool.prev_is_me_leader
		get_leader = self.pool.prev_get_my_leader
	else
		self.utils.error('Unknown schema name: %s. Valid values are: [curr, prev]', schema)
	end
		
	if not is_leader(self.pool) then
		local node = get_leader(self.pool)
		if node == nil then
			tuple = __get(node.conn, space, index, key)
		end
		
		if tuple then return tuple end
	end
	
	return tuple
end

function raftshard._count_me(schema, space, index, key, opts)
	-- print('Got _count_me request from', box.session.peer())
	local count = __count(box, space, index, key)
	if count and count > 0 then return count end
	
	local is_leader
	local get_leader
	if schema == 'curr' then
		is_leader = self.pool.curr_is_me_leader
		get_leader = self.pool.curr_get_my_leader
	elseif schema == 'prev' then
		is_leader = self.pool.prev_is_me_leader
		get_leader = self.pool.prev_get_my_leader
	else
		self.utils.error('Unknown schema name: %s. Valid values are: [curr, prev]', schema)
	end
		
	if not is_leader(self.pool) then
		local node = get_leader(self.pool)
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

function raftshard._update_me(space, index, key, upd)
	return __update(box, space, index, key, upd)
end



function raftshard.select(space, index, key, opts)
	-- box.shard.select('tokens', 'primary', {'token-id-1'})
	self.check_space_and_index(space, index)
	
	local shard_id = self.schema:curr(space, index, key)
	
	if self.pool:curr_is_me_by(shard_id) then
		local tuples = self._select_me('curr', space, index, key, opts)
		if tuples and #tuples > 0 then return tuples end
		
	elseif shard_id == self.schema.ALL_SHARDS then
		local results = self.execute_on_many_shards('curr', 'box.shard._select_me_unpack', 'curr', space, index, key, opts)
		if results == nil then
			box.error(box.error.TIMEOUT)
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
	
	log.info("Fall back to prev schema")
	if not self.schema.prev then return end
	
	shard_id = self.schema:prev(space, index, key)
	
	if self.pool:prev_is_me_by(shard_id) then
		return self._select_me('prev', space, index, key, opts)
		
	elseif shard_id == self.schema.ALL_SHARDS then
		local results = self.execute_on_many_shards('prev', 'box.shard._select_me_unpack', 'prev', space, index, key, opts)
		if results == nil then
			box.error(box.error.TIMEOUT)
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
		local node = self.pool:prev_leader_by(shard_id) or self.pool:prev_by(shard_id)
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
		local tuple = self._get_me('curr', space, index, key)
		if tuple then return tuple end
		
	elseif shard_id == self.schema.ALL_SHARDS then
		local results = self.execute_on_many_shards('curr', 'box.shard._get_me', 'curr', space, index, key)
		if results == nil then
			box.error(box.error.TIMEOUT)
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
	
	log.info("Fall back to prev schema")
	if not self.schema.prev then return end
	
	
	local shard_id = self.schema:prev(space, index, key)
	
	if self.pool:prev_is_me_by(shard_id) then
		return self._get_me('prev', space, index, key)
		
	elseif shard_id == self.schema.ALL_SHARDS then
		local results = self.execute_on_many_shards('prev', 'box.shard._get_me', 'prev', space, index, key)
		if results == nil then
			box.error(box.error.TIMEOUT)
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
		return tuple
	else
		-- not my shard. we need to make request to another shard's leader
		local node = self.pool:prev_leader_by(shard_id) or self.pool:prev_by(shard_id)
		if node then
			return __get(node.conn, space, index, key)
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
		local count = self._count_me('curr', space, index, key, opts)
		if count and count > 0 then return count end
	elseif shard_id == self.schema.ALL_SHARDS then
		local results = self.execute_on_many_shards('curr', 'box.shard._count_me', 'curr', space, index, key, opts)
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
	
	log.info("Fall back to prev schema")
	if not self.schema.prev then return end
	
	local shard_id = self.schema:prev(space, index, key)
	
	if self.pool:prev_is_me_by(shard_id) then
		local count = self._count_me('prev', space, index, key, opts)
		if count and count > 0 then return count end
	elseif shard_id == self.schema.ALL_SHARDS then
		local results = self.execute_on_many_shards('prev', 'box.shard._count_me', 'prev', space, index, key, opts)
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
		return total_count
	else
		-- not my shard. we need to make request to another shard's leader
		local node = self.pool:prev_leader_by(shard_id) or self.pool:prev_by(shard_id)
		if node then
			return __count(node.conn, space, index, key, opts)
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
	
	
	local shard_id = self.schema:curr(space, self.shard_index, tuple)  -- getting shard_id for self.shard_index and not primary index
	
	if self.pool:curr_is_me_leader_by(shard_id) then
		if self.schema.prev == nil then
			return __insert(box, space, tuple)
		end
		
		local key = self.utils.extract_key_by_index(space, index, tuple)
		local tuples = __select(box, space, index, key)
		local is_index_unique = self.utils.is_index_unique(space, index)
		if (tuples ~= nil and #tuples ~= 0) and is_index_unique then
			box.error(box.error.TUPLE_FOUND, index, space)
		end
		
		local prev_shard_id = self.schema:prev(space, self.shard_index, tuple)
		if self.pool:curr_is_me_leader_by(shard_id) and self.pool:prev_is_me_leader_by(prev_shard_id) then
			return __insert(box, space, tuple)
		end
		
		local node = self.pool:prev_leader_by(prev_shard_id)
		if not node then self.utils.error('Leader of shard #%d is not available ', prev_shard_id) end
		
		tuples = __select(node.conn, space, index, key)
		if (tuples ~= nil and #tuples ~= 0) and is_index_unique then
			box.error(box.error.TUPLE_FOUND, index, space)
		end
		
		return __insert(box, space, tuple)
	elseif shard_id == self.schema.ALL_SHARDS then
		self.utils.error('Cannot execute insert on all shards')
	else
		self.check_ttl(shard_id, ttl)
		local node = self.pool:curr_leader_by(shard_id)
		if node then
			return unpack(node.conn:call('box.shard._insert', ttl - 1, space, index, tuple))
		end
		self.utils.error('Leader of shard #%d is not accessible', shard_id)
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
	
	local shard_id = self.schema:curr(space, self.shard_index, tuple)
	
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
	self.check_space_and_index(space, index)
	
	ttl = tonumber(ttl)
	
	local shard_id = self.schema:curr(space, index, key)
	
	if self.pool:curr_is_me_by(shard_id) then
		local ptuple
		if self.schema.prev then
			local prev_shard_id = self.schema:prev(space, index, key)
			local node = self.pool:prev_leader_by(prev_shard_id)
			if node and node.conn then
				ptuple = __delete(node.conn, space, index, key)
			else
				log.error("Could not call delete key %s:%s:{ %s } from prev shard %d: not accessible", space, index, table.concat(key, ' '), prev_shard_id)
			end
		end
		if not ptuple then
			return __delete(box, space, index, key)
		else
			local tuple = __delete(box, space, index, key)
			return tuple or ptuple
		end
		return self._delete_me(space, index, key)
	elseif shard_id == self.schema.ALL_SHARDS then
		local results = self.execute_on_many_shards('curr', 'box.shard._delete_me', space, index, key)
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

function raftshard.update(space, index, key, upd) return self._update(1, space, index, key, upd) end
function raftshard._update(ttl, space, index, key, upd)
	self.check_space_and_index(space, index)
	ttl = tonumber(ttl)
	
	if key == nil then
		key = {}
	end
	
	local index_parts_count = self.utils.index_parts_count(space, index)
	if #key ~= index_parts_count then
		box.error(box.error.EXACT_MATCH, index_parts_count, #key)
	end
	
	local shard_id = self.schema:curr(space, index, key)
	if self.pool:curr_is_me_leader_by(shard_id) then
		local tuple = __get(box, space, index, key)
		
		if tuple or not self.schema.prev then
			return __update(box, space, index, key, upd)
		end
		
		local prev_shard_id = self.schema:prev(space, index, key)
		
		if self.pool:curr_is_me_leader_by(shard_id) and self.pool:prev_is_me_leader_by(prev_shard_id) then
			return __update(box, space, index, key, upd)
		end
		
		local node = self.pool:prev_leader_by(prev_shard_id)
		if node and node.conn then
			local ptuple = __get(node.conn, space, index, key)
			if not ptuple then
				return __update(box, space, index, key, upd)
			end
			
			-- rechek
			tuple = __get(box, space, index, key)
			if tuple then
				return __update(box, space, index, key, upd)
			end
			__insert(box, space, ptuple)
			return __update(box, space, index, key, upd)
		else
			log.error("Could not call update key %s:%s{ %s } from prev shard %d: not accessible", space, index, table.concat(key, ' '), prev_shard_id)
		end
	elseif shard_id == self.schema.ALL_SHARDS then
		self.utils.error('Cannot perform update on all shards (probably you\'ve tried to update not by primary index)')
	else
		self.check_ttl(shard_id, ttl)
		local node = self.pool:curr_leader_by(shard_id)
		if node then
			return unpack(node.conn:call('box.shard._update', ttl - 1, space, index, key, upd))
		end
		self.utils.error('Shard #%d leader is not accessible', shard_id)
	end
end


function raftshard.execute_on_many_shards(schema, method, space, index, ...)
	local nodes = {}
	local length
	local node_getter
	if schema == 'curr' then
		length = self.schema.curr_length
		node_getter = self.pool.curr_by
	else
		length = self.schema.prev_length
		node_getter = self.pool.prev_by
	end
	
	for shard_id = 1, length do
		local node = node_getter(self.pool, shard_id)
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
		update = function(_, key, upd)
			return raftshard.update(for_space, for_index, key, upd)
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
