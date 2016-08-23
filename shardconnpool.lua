local fiber = require('fiber')
local log = require('log')
local msgpack = require('msgpack')
local nbox = require('net.box')
local obj = require('obj')
local uuid = require('uuid')
local yaml = require('yaml')

local _connpool = require('connpool')
local pool = obj.class({}, 'shardconnpool', _connpool)

function pool:_init(cfg, schema)
	self.total = 0
	self.timeout = cfg.timeout or 1
	self.func_timeout = cfg.func_timeout or 3
	self.name = cfg.name or 'default'
	self.raft_debug = cfg.raft_debug or false
	self.schema = schema
	
	self.rafts = {}
	self.nodes_by_peer = {}
	self.nodes_by_uuid = {}
	self.nodes_by_state = {}
	
	self.all = {}                 -- { schema -> { shard_id -> [array of active nodes] } }
	self.global_groups = {}       -- { uuid -> [array of nodes of the group] }
	self.global_groups_refs = {}  -- { schema -> { shard_id -> global_gid } }
	
	local cl = self.schema.curr_list
	local pl = self.schema.prev_list
	local schemas = {
		curr = cl,
	}
	if pl and pl ~= cl then
		schemas.prev = pl
	end
	
	
	local all = {}
	local global_groups_refs = {}
	for schema, schema_config in pairs(schemas) do
		for shard_id, shard_config in ipairs(schema_config) do
			for _,server_config in ipairs(shard_config) do
				local node = self:init_node(server_config, cfg)
				
				all = all or {}
				all[schema] = all[schema] or {}
				all[schema][shard_id] = all[schema][shard_id] or {}
				
				table.insert(node.roles, {schema, shard_id})
			end
		end
	end
	
	for peer, node in pairs(self.nodes_by_peer) do
		global_gid = self.schema:get_global_gid(peer)
		node.global_gid = global_gid
		
		self.global_groups = self.global_groups or {}
		self.global_groups[global_gid] = self.global_groups[global_gid] or {}
		
		for _, role in pairs(node.roles) do
			local schema, shard_id = unpack(role)
			
			global_groups_refs = global_groups_refs or {}
			global_groups_refs[schema] = global_groups_refs[schema] or {}
			global_groups_refs[schema][shard_id] = global_gid
		end
		
		table.insert(self.global_groups[global_gid], node)
	end
	
	self.all = all
	self.global_groups_refs = global_groups_refs
	
	self.me = {} -- curr = {}; prev = {};
	for _, schema in pairs({'curr', 'prev'}) do
		self.me[schema] = { hash = {}; list = {} }
	end
	
	for _, state in ipairs({'active', 'deferred', 'inactive'}) do
		self.nodes_by_state[state] = {}
	end
	for _, node in pairs(self.nodes_by_peer) do
		table.insert(self.nodes_by_state.inactive, node)
	end
	
	self.self_node = {
		connected = false
	}
	
	self.on_connected_one = pool.on_connected_one
	self.on_connected = pool.on_connected
	self.on_connfail = pool.on_connfail
	self.on_disconnect_one = pool.on_disconnect_one
	self.on_disconnect = pool.on_disconnect
	
	
	self.__connecting = false
	self._counts = nil
end

function pool:init_node(srv, cfg)
	local login = srv.login or cfg.login
	local password = srv.password or cfg.password
	
	local uri
	if login and password then
		uri = login .. ':' .. password .. '@' .. srv.uri
	else
		uri = srv.uri
	end
	
	local peer = srv.uri
	local node
	if not self.nodes_by_peer[peer] then
		node = {
			peer = srv.uri,
			uri  = uri,
			connected = false,
			connected_once = false,
			state = 'inactive',
			rafted = false,
			roles = {},
			global_gid = msgpack.NULL,
			
			-- params below are filled after connect (N.B.: would love to use msgpack.NULL but it's not working properly as nil)
			conn = nil,
			fiber = nil,
			uuid = nil,
			id = nil,
			raft = nil,
			is_leader = function(self)
				if self.raft.is_leader ~= nil then
					return self.raft:is_leader()
				end
				return false
			end
		}
		self.nodes_by_peer[peer] = node
	else
		node = self.nodes_by_peer[peer]
	end
	
	return node
end

function pool:counts()
	if self._counts then return self._counts end
	self._counts = {
		active   = #self.nodes_by_state.active;
		inactive = #self.nodes_by_state.inactive;
		deferred = #self.nodes_by_state.deferred;
	}
	return self._counts
end

function pool:_move_node(node, state1, state2)
	if state1 == state2 then
		log.debug("Tried to move node to the same state as before: %s", state1)
		return
	end
	
	local found = false
	log.info("move node %s from %s to %s", node.peer, state1, state2)
	if self.nodes_by_state[state1] ~= nil then
		for _,v in pairs(self.nodes_by_state[state1]) do
			if v == node then
				table.remove(self.nodes_by_state[state1], _)
				found = true
				break
			end
		end
		if not found then
			log.error("Node %s not found in state %s.", node.peer, state1)
		end
		self.nodes_by_state[state2] = self.nodes_by_state[state2] or {}
		table.insert(self.nodes_by_state[state2], node)
	else
		log.error("nodes_by_state['%s'] does not exist", state1)
	end
	node.state = state2
end

function pool:node_state_change(node, state)
	local prevstate = node.state
	self._counts = nil

	if state == 'active' then
		self:_move_node(node, prevstate, state)
		
		node.connected = true
		for _,role in pairs(node.roles) do
			local schema, shard_id = unpack(role)
			for _,one in pairs(self.all[schema][shard_id]) do
				if one == node then
					error("FATAL! Contact developer. Node "..node.key.." already exists for "..schema.."/"..mode.."/"..shno)
				end
			end
			table.insert(self.all[schema][shard_id], node)
		end
		
		if not node.connected_once then
			node.connected_once = true
			self:_on_node_first_connection(node)
		end
		
		self:_on_connected_one(node)
		self.on_connected_one(node)
		
		if self:counts().active == self.total then
			self.on_connected()
		end
	else -- deferred or inactive
		self:_move_node(node, prevstate, state)
		-- moving from deferred to inactive we don't alert with callbacks
		node.connected = false
		
		if prevstate == 'active' then
			
			for _,role in pairs(node.roles) do
				local schema, shard_id = unpack(role)
				for pos,one in pairs(self.all[schema][shard_id]) do
					if one == node then
						table.remove(self.all[schema][shard_id], pos)
						break
					end
				end
			end
			
			self:_on_disconnect_one(node)
			self.on_disconnect_one(node)

			if self:counts().active == 0 then
				self.on_disconnect()
			end
		end
	end
end

function pool:connect()
	if self.__connecting then return end
	self.__connecting = true
	self:on_init()
	local self_uuid = box.info.server.uuid
	
	for _,node in pairs(self.nodes_by_peer) do
		node.fiber = fiber.create(function()
			fiber.name(self.name..':'..node.peer)
			node.conn = nbox.new( node.uri, { reconnect_after = 1/3, timeout = 1 } )
			local state
			local conn_generation = 0
			while true do
				state = node.conn:_wait_state({active = true, activew = true})

				local r,e = pcall(node.conn.eval, node.conn, "return box.info")
				if r and e then
					if type(e) ~= 'table' then
						log.warn('WEIRD RESPONSE: %s:%s', tostring(r), tostring(e))
					end
					local uuid = e.server.uuid
					local id = e.server.id
					log.info("connected %s, uuid: %s", node.peer, uuid)
					conn_generation = conn_generation + 1
					
					self:_check_uuid_id(node, uuid, id)
					
					if uuid == self_uuid then  -- it's me
						self.self_node = node
						node.conn:close()
						node.conn = nbox.self
						
						for _, role in pairs(node.roles) do
							local schema, shard_id = unpack(role)
							
							table.insert(self.me[schema].list, shard_id)
							self.me[schema].hash[shard_id] = node
						end
						
						self:node_state_change(node, 'active')
						break
					end
					
					self:node_state_change(node, 'active')

					-- FIXME: uncomment pinger when tarantool net_box is resolved
					--- start pinger
					-- fiber.create(function()
					-- 	local gen = conn_generation
					-- 	local last_state_ok = true
					-- 	while gen == conn_generation do
							
					-- 		-- local r,e = pcall(node.conn.eval, node.conn:timeout(self.timeout), "return box.info")
					-- 		local r,e = pcall(node.conn.ping,node.conn:timeout(self.timeout))
					-- 		if r and e then
					-- 			-- self:_check_uuid_id(node, e.server.uuid, e.server.id)
								
					-- 			if not last_state_ok and gen == conn_generation then
					-- 				log.info("node %s become online by ping", node.peer)
					-- 				last_state_ok = true
					-- 				self:node_state_change(node, 'active')
					-- 			end
					-- 		else
					-- 			if last_state_ok and gen == conn_generation then
					-- 				log.warn("node %s become offline by ping: %s", node.peer, e)
					-- 				last_state_ok = false
					-- 				self:node_state_change(node, 'deferred')
					-- 			end
					-- 		end
					-- 		fiber.sleep(1)
					-- 	end
					-- end)

					state = node.conn:_wait_state({error = true, closed = true})
					self:node_state_change(node, 'inactive')
				else
					log.warn("box.info request failed for %s: %s", node.peer, e)
				end
			end

		end)
	end
end

function pool:_on_node_first_connection(node)
	if node.rafted then return end -- determining if raft is already initialized for the selected group
	
	-- N.B.: If current node is not connected then we cannot determine if it is in raft_group or not,
	-- because uuid in node appears only after successful connect therefore we cannot start any raft in this condition
	-- This implies that current node MUST be in servers list in configuration.
	if not self.self_node.connected then
		log.info('Cannot init raft - current node (%s) is not connected yet.', box.info.server.uuid)
		return
	end
	
	-- getting group that node is in
	local group_id = node.global_gid
	local raft_group = self.global_groups[group_id]
	
	log.info('Starting raft init for node: [%s/%s], group_id: %s', node.peer, node.uuid, group_id)
	
	-- determining if current node (box.info.server.uuid) is present in selected group
	local current_node_in_group = false
	for _,n in ipairs(raft_group) do
		if n.uuid ~= nil then
			if n.uuid == box.info.server.uuid then
				current_node_in_group = true
			end
		end
	end
	
	-- if all_uuids_nil and #raft_group ~= 1 then
	-- 	log.error("Couldn't init raft because of lack of uuid information. node.peer=%s; group_id=%s; current_node_in_group=%s; nodes_count=%d;", node.peer, group_id, current_node_in_group, #raft_group)
	-- 	return
	-- end
	
	if self.rafts[group_id] ~= nil then
		log.info('Raft is already initialized for group_id=%s', group_id)
		for _,n in pairs(raft_group) do
			n.rafted = true
			n.raft = self.rafts[group_id]
		end
		return
	end
	
	log.info("Initializing raft for node.peer=%s; group_id=%s; current_node_in_group=%s;", node.peer, group_id, current_node_in_group)
	
	-- initilizing either raftsrv or raftconn
	local raft = nil
	if current_node_in_group then
		local raftsrv = require('raft-srv')
		raft = raftsrv({
			debug = self.raft_debug,
			conn_pool = self,
			nodes = raft_group,
		})
	else
		local raftconn = require('raft-conn')
		raft = raftconn({
			debug = self.raft_debug,
			conn_pool = self,
			nodes = raft_group,
		})
	end
	
	
	-- setting flags that raft is created for each node in the group
	for _,n in pairs(raft_group) do
		n.rafted = true
		n.raft = raft
	end
	
	self.rafts[group_id] = raft
	
	-- starting rafts
	raft:start()
end

-------------- <Node Getters> --------------

function pool:get_by_uuid(uuid)
	return self.nodes_by_uuid[uuid]
end

function pool:get_by_peer(peer)
	return self.nodes_by_peer[peer]
end

-------------- </Node Getters> --------------

-------------- <curr> --------------

function pool:curr_leaders_by(shard_id)
	if shard_id == self.schema.ALL_SHARDS then
		box.error{reason = 'curr_leaders not allowed on ALL_SHARDS('..tostring(self.schema.ALL_SHARDS) .. ')'}
	end
	
	local global_gid = self.global_groups_refs.curr[shard_id]
	if not global_gid then
		box.error({reason = 'Unknown shard_id: ' .. tostring(shard_id)})
	end
	local leaders = self.rafts[global_gid]:get_leader_nodes()
	return leaders
end

function pool:curr_leaders(...)
	local shard_id = self.schema:curr(...)
	return self:curr_leaders_by(shard_id)
end

function pool:curr_leader_by(shard_id)
	local nodes = self:curr_leaders_by(shard_id)
	if nodes == nil or #nodes == 0 then
		return nil
	end
	return nodes[ math.random(1, #nodes) ]
end

function pool:curr_leader(...)
	local shard_id = self.schema:curr(...)
	return self:curr_leader_by(shard_id)
end

function pool:curr_by(shard_id)
	local nodes = self.all.curr[shard_id]
	if nodes == nil or #nodes == 0 then
		return nil
	end
	return nodes[ math.random(1, #nodes) ]
end

function pool:curr(...)
	local shard_id = self.schema:curr(...)
	return self:curr_by(shard_id)
end

-------------- </curr> --------------

-------------- <prev> --------------

function pool:prev_leaders_by(shard_id)
	if shard_id == self.schema.ALL_SHARDS then
		box.error{reason = 'prev_leaders not allowed on ALL_SHARDS('..tostring(self.schema.ALL_SHARDS) .. ')'}
	end
	
	local global_gid = self.global_groups_refs.prev[shard_id]
	if not global_gid then
		box.error({reason = 'Unknown shard_id: ' .. tostring(shard_id)})
	end
	local leaders = self.rafts[global_gid]:get_leader_nodes()
	return leaders
end

function pool:prev_leaders(...)
	local shard_id = self.schema:prev(...)
	return self:prev_leaders_by(shard_id)
end

function pool:prev_leader_by(shard_id)
	local nodes = self:prev_leaders_by(shard_id)
	if nodes == nil or #nodes == 0 then
		return nil
	end
	return nodes[ math.random(1, #nodes) ]
end

function pool:prev_leader(...)
	local shard_id = self.schema:prev(...)
	return self:prev_leader_by(shard_id)
end

function pool:prev_by(shard_id)
	local nodes = self.all.prev[shard_id]
	if nodes == nil or #nodes == 0 then
		return nil
	end
	return nodes[ math.random(1, #nodes) ]
end

function pool:prev(...)
	local shard_id = self.schema:prev(...)
	return self:prev_by(shard_id)
end

-------------- </prev> --------------

-------------- <curr me> --------------

function pool:curr_get_my_node_by(shard_id)
	return self.me.curr.hash[shard_id]
end

function pool:curr_is_me_by(shard_id)
	local node = self.me.curr.hash[shard_id]
	if node == nil then
		return false
	end
	return true
end

function pool:curr_is_me_leader_by(shard_id)
	local node = self.me.curr.hash[shard_id]
	if node == nil then
		return false
	end
	return node:is_leader()
end

function pool:curr_get_my_shard_id()
	local shard_ids = self.me.curr.list
	if #shard_ids > 1 then
		log.warn('Have multiple shard_ids for my node in curr schema')
		return shard_ids[ math.random(1, #shard_ids) ]
	elseif #shard_ids == 0 then
		log.warn('Have no shard_ids for my node in curr schema')
		return nil
	else
		return shard_ids[1]
	end
end

function pool:curr_get_my_node()
	local shard_id = self:curr_get_my_shard_id()
	return self:curr_get_my_node_by(shard_id)
end

function pool:curr_get_my_leader()
	local shard_id = self:curr_get_my_shard_id()
	return self:curr_leader_by(shard_id)
end

function pool:curr_is_me()
	local shard_id = self:curr_get_my_shard_id()
	return self:curr_is_me_by(shard_id)
end

function pool:curr_is_me_leader()
	local shard_id = self:curr_get_my_shard_id()
	return self:curr_is_me_leader_by(shard_id)
end


-------------- </curr me> --------------

-------------- <prev me> --------------

function pool:prev_get_my_node_by(shard_id)
	return self.me.prev.hash[shard_id]
end

function pool:prev_is_me_by(shard_id)
	local node = self.me.prev.hash[shard_id]
	if node == nil then
		return false
	end
	return true
end

function pool:prev_is_me_leader_by(shard_id)
	local node = self.me.prev.hash[shard_id]
	if node == nil then
		return false
	end
	return node:is_leader()
end

function pool:prev_get_my_shard_id()
	local shard_ids = self.me.prev.list
	if #shard_ids > 1 then
		log.warn('Have multiple shard_ids for my node in prev schema')
		return shard_ids[ math.random(1, #shard_ids) ]
	elseif #shard_ids == 0 then
		log.warn('Have no shard_ids for my node in prev schema')
		return nil
	else
		return shard_ids[1]
	end
end

function pool:prev_get_my_node()
	local shard_id = self:prev_get_my_shard_id()
	return self:prev_get_my_node_by(shard_id)
end

function pool:prev_get_my_leader()
	local shard_id = self:prev_get_my_shard_id()
	return self:prev_leader_by(shard_id)
end

function pool:prev_is_me()
	local shard_id = self:prev_get_my_shard_id()
	return self:prev_is_me_by(shard_id)
end

function pool:prev_is_me_leader()
	local shard_id = self:prev_get_my_shard_id()
	return self:prev_is_me_leader_by(shard_id)
end

-------------- </prev me> --------------


function pool:_on_connected_one(node)
	local global_gid = node.global_gid
	local raft = self.rafts[global_gid]
	if raft ~= nil then
		log.info("[pool.on_connected_one] Calling raft:on_connected_one on node %s connected", node.peer)
		raft:_pool_on_connected_one(node)
		node.notified_raft_connected = true
		
		for _,n in pairs(self.global_groups[global_gid]) do
			if n.connected and n.notified_raft_connected == nil then
				raft:_pool_on_connected_one(n)
				n.notified_raft_connected = true
			end
		end
	else
		log.warn("[pool.on_connected_one] Raft for group %s not found", global_gid)
	end
end

function pool:_on_disconnect_one(node)
	local global_gid = node.global_gid
	local raft = self.rafts[global_gid]
	if raft ~= nil then
		log.info("[pool.on_disconnected_one] Calling raft:on_disconnected_one on node %s disconnected", node.peer)
		raft:_pool_on_disconnect_one(node)
		node.notified_raft_connected = nil
	else
		log.warn("[pool.on_disconnect_one] Raft for group %s not found", global_gid)
	end
end

return pool
