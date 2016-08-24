local obj = require('obj')
local log = require('log')
local msgpack = require('msgpack')
local uuid = require('uuid')
local yaml = require('yaml')
local digest = require('digest')

local M = obj.class({}, 'shardschema')

function M:_init(cfg)
	self.ALL_SHARDS = -1
	self:configure(cfg)
end

function M:configure(cfg)
	local cl,pl,cf,pf = cfg.curr_list,
						cfg.prev_list,
						cfg.func or cfg.curr_func or self.default_extract_key,
						cfg.prev_func or self.default_extract_key
	
	if not cl or #cl < 1 then error("Non empty `curr_list' required") end
	if not cf then error("Defined `curr' function required") end
	
	self.curr_list = cl
	self.curr_func = cf
	self.curr_length = #self.curr_list
	
	self.curr = function(self, space, index, key)
		if not ((index == 'primary' or index == 0) and key ~= nil and #key > 0) then
			return self.ALL_SHARDS
		end
		
		local shard_id = self.curr_func(self, space, index, key)
		if shard_id == nil then  -- choosing random shard
			shard_id = math.random(1, #self.curr_list)
			return shard_id
		end
		print('shard_id: ' .. tostring(shard_id))
		if shard_id > 0 and shard_id <= #self.curr_list then
			return shard_id
		else
			error("curr function returned wrong shard no: "..tostring(shno).."; avail range is: [1.."..tostring(#self.curr_list))
		end
	end
	
	if pl and pf then
		self.prev_list = pl
		self.prev_func = pf
		self.prev_length = #self.prev_list
		
		self.prev = function(self, space, index, key)
			if not ((index == 'primary' or index == 0) and key ~= nil and #key > 0) then
				return self.ALL_SHARDS
			end
			
			local shard_id = self.prev_func(self, space, index, key)
			if shard_id == nil then  -- choosing random shard
				shard_id = math.random(1, #self.prev_list)
				return shard_id
			end
			print('shard_id: ' .. tostring(shard_id))
			if shard_id > 0 and shard_id <= #self.prev_list then
				return shard_id
			else
				error("prev function returned wrong shard no: "..tostring(shno).."; avail range is: [1.."..tostring(#self.prev_list))
			end
		end
	end
	
	self.global_group_peers = {}
	self.global_group_peers_refs = {}
	self:_setup_global_groups()
end

function M:shard(key)
	-- main shards search function (from https://github.com/tarantool/shard/blob/master/shard.lua)
	local num
	if type(key) == 'number' then
		num = key
	else
		num = digest.crc32(key)
	end
	local shard_id = 1 + digest.guava(num, #self.curr_list)
	return shard_id
end

function M.default_extract_key(self, space, index, key)
	return self:shard(key[1])
end


function M:get_global_gid(peer)
	return self.global_group_peers_refs[peer]
end

function M:_setup_global_groups()
	-- local unique_groups = self.curr_list
	local unique_groups = self:make_unique_groups(self.curr_list, self.prev_list)
	for _, group in pairs(unique_groups) do
		local global_gid = uuid():str()
		
		self.global_group_peers[global_gid] = group
		for _, peer in ipairs(group) do
			self.global_group_peers_refs[peer.uri] = global_gid
		end
	end
end

local function nodes_group_hash(group)
	table.sort(group)
	local h = table.concat(group, ',')
	h = digest.md5(h)
	return h
end

local function extract_uris_from_group(group)
	-- TODO: if uri will not be the only field in node definition tweak this functoin
	local uri_arr = {}
	for _, n in pairs(group) do
		assert(n.uri ~= nil, "Schema misconfiguration: uri for node must be defined")
		table.insert(uri_arr, n.uri)
	end
	return uri_arr
end

function M:make_unique_groups(curr_list, prev_list)
	local unique_groups = {}
	local unique_groups_hash = {}
	local hashes_per_list = {}
	for list_id, list in ipairs({curr_list, prev_list}) do
		local list_name
		if list_id == 1 then
			list_name = 'curr'
		else
			list_name = 'prev'
		end
		hashes_per_list[list_name] = {}
		
		local seen_peers_in_list = {}
		local unique_groups_hash_in_list = {}
		if type(list) ~= 'table' then errorf('Schema misconfiguration: list definition must be a table, got: %s for %s list', type(group), list_name) end
		if #list == 0 then errorf('Schema misconfiguration: list definition must be non-empty (%s list)', list_name) end
		for group_id, group in pairs(list) do
			if type(group) ~= 'table' then errorf('Schema misconfiguration: Shard definition must be a table, got: %s', type(group)) end
			if #group == 0 then errorf('Schema misconfiguration: group definition must be non-empty (group %s in list %s)', tostring(group_id), list_name) end
			
			local group_uris = extract_uris_from_group(group)
			
			local group_hash = nodes_group_hash(group_uris)
			if unique_groups_hash_in_list[group_hash] then
				errorf('Schema misconfiguration: group definition must occur only once (list %s)', list_name)
			end
			if not unique_groups_hash[group_hash] then
				table.insert(unique_groups, group)
				unique_groups_hash[group_hash] = true
				unique_groups_hash_in_list[group_hash] = true
			end
			table.insert(hashes_per_list[list_name], group_hash)
			
			for _, peer in ipairs(group_uris) do
				-- One node may be in list configuration only once
				if seen_peers_in_list[peer] then
					errorf('Schema misconfiguration: peer %s is present multiple times in %s list', peer, list_name)
				end
				seen_peers_in_list[peer] = true
			end
		end
		table.sort(hashes_per_list[list_name])
	end
	
	-- Check if schemas are the same
	if hashes_per_list.prev and #hashes_per_list.curr == #hashes_per_list.prev then
		local are_same = true
		for i, group_hash in pairs(hashes_per_list.curr) do
			if group_hash ~= hashes_per_list.prev[i] then
				are_same = false
			end
		end
		if are_same then
			errorf('Schema misconfiguration: curr and prev schemas are identical. Either change schema config or remove prev schema.')
		end
	end
	
	-- One peer must not be in multiple unique groups
	local seen_peers = {}
	for _,group in ipairs(unique_groups) do
		local group_uris = extract_uris_from_group(group)
		for _, peer in ipairs(group_uris) do
			-- One node may be in list configuration only once
			if seen_peers[peer] then
				errorf('Schema misconfiguration: peer %s is present in multiple unique groups. Groups representing the same shard must match in curr and prev schemas.', peer)
			end
			seen_peers[peer] = true
		end
	end
	
	return unique_groups
end

function errorf(s, ...)
	error(string.format(s, ...))
end

return M
