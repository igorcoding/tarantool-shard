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
	
	self.prev_list = pl
	self.prev_func = prev_func
	
	
	self.curr_length = #self.curr_list
	if self.prev_list ~= nil then
		self.prev_length = #self.prev_list
	else
		self.prev_length = 0
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
	-- TODO: find unique groups. Just for now it will only curr schema, so no groups from prev schema will be available
	local unique_groups = self.curr_list
	for _, group in pairs(unique_groups) do
		local global_gid = uuid():str()
		
		self.global_group_peers[global_gid] = group
		for _, peer in ipairs(group) do
			self.global_group_peers_refs[peer.uri] = global_gid
		end
	end
end

return M
