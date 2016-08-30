local fiber = require('fiber')
local json = require('json')
local log = require('log')
local msgpack = require('msgpack')

local obj = require('obj')

local M = obj.class({}, 'resharding')

function M:_init(pool, cfg)
	log.info('Init ressharding module')
	
	if not cfg then
		cfg = {}
	end
	
	if cfg.spaces == nil or #cfg.spaces == 0 then
		error('Need to define spaces which are needed for resharding')
	end
	
	self.pool = pool
	fiber.create(function(self)
		box.shard.waitonline()
		
		--if not box.cfg.replication_source then
		-- if box.info().status ~= 'primary' then
		-- 	log.info("I am not master. A can't do reshard.")
		-- 	return 0
		-- end

		-- log.info("I am master. A want do reshard!")
		self.waitevery     = cfg.waitevery or 2 -- 1000
		self.printevery    = cfg.printevery or 1 -- 10000
		self.migrateby     = cfg.migrateby or 1 -- 1000
		self.spaces        = cfg.spaces
		self.primary_index = cfg.primary_index or 'primary'
		
		self.tomigrate     = {}
		self.fiber         = {}
		self.done          = false
		
		-- get space
		-- for _,s in pairs(self.spaces) do
		-- 	self:restart(s)
		-- end
		
	end, self)
end

function M:restart_all()
	for _,s in pairs(self.spaces) do
		self:restart(s)
	end
end

function M:restart(space)
	if self.fiber and self.fiber[space] then
		self:stop(space)
	end
	self:start(space)
end

function M:stop(space)
	if self.fiber[space] then
		local r,id = pcall(function() return self.fiber[space]:id() end)
		if r then
			log.info("Stopping fiber with id=%s for space=%s", tostring(id), tostring(space))
			self.fiber[space]:cancel()
			self.fiber[space] = nil
		else
			log.error("Can't cancel old fiber with for space=%s", tostring(space))
			self.fiber[space] = nil
		end
	else
		log.error("Fiber is nil for space=", space)
	end
end

function M:start(space)
	local space_fiber = fiber.create(function()
		fiber.self():name('resharding.space.', space)
		fiber.sleep(0)
		self:runit(space)
	end)
	self.fiber[space] = space_fiber
	log.info("Resharding is run: for space[%s] created background fiber: %d",tostring(space), space_fiber:id());
	log.info("Resharding for space[%s] can be cancelled with `require('fiber').kill(%d)`", tostring(space), space_fiber:id())
end

function M:needmigration(space, index, key, pkey)
	if self.done then log.info("[RSH][needmigrate] self.done: return false") return false end
	if self.tomigrate[space] and self.tomigrate[space][pkey] then log.info("[RSH][needmigrate] inprogress: return false") return false end
	local curr = self.pool.schema:curr(space, index, key)
	local prev = self.pool.schema:prev(space, index, key)
	if curr == prev then
		log.info("[RSH][needmigrate] curr==prev: return false")
		return false
	else
		if self.pool:curr_is_me_by(curr) then
			log.info("[RSH][needmigrate] curr is me: return false")
			return false
		else
			return true
		end
	end
end


function M:mprocess(space, index, key)
	local pnode = self.pool:prev_leader(space, index, key)
	if not pnode or not pnode.conn then
		log.error('No prev leader conn found for space=%s and key=%s', space, box.tuple.new{key})
		return false
	end
	local pconn = pnode.conn.space[space].index[index]
	
	local v
	local r,e = pcall(pconn.get, pconn, key)
	if r then
		v = e
	else
		log.error("Can't get tuple in prev space=%s/%s: %s", space, box.tuple.new{key}, e)
		return false
	end
	
	if v == nil then
		log.error("Can't get tuple in prev space=%s: %s", space, box.tuple.new{key})
		return false
	end
	
	local node = self.pool:curr_leader(space, index, key)
	if not node or not node.conn then
		log.error('No curr leader conn found for space=%s and key=%s', space, box.tuple.new{key})
		return false
	end
	local conn = node.conn.space[space]
	r,e = pcall(conn.insert, conn, v)
	log.info("[RSH] mp: conn:insert = %s %s", r, json.encode(e))
	
	if not r and string.match(tostring(e),"Duplicate") then
		log.info("Duplicate in curr space[%s]: %s", space, box.tuple.new{key})
	end
	if r or string.match(tostring(e),"Duplicate") then
		-- local local_conn = box.space[space].index[index]
		local r,e = pcall(pconn.delete, pconn, key)
		if r then
			return true
		else
			log.error("Can't delete in prev space[%s]: %s: %s", space, box.tuple.new{key}, e)
			return false
		end
	else
		log.error("Can't insert in curr space[%s]: %s: %s", space, box.tuple.new{key}, e)
		return false
	end
end

function M:packkey(key)
	return msgpack.encode(key)
end

local function iter(index, ...)
	local f,s,var = index:pairs(...)
	local iterator = {
		f = f,
		s = s,
		var = var,
	}

	return setmetatable(iterator, {
		__call = function(self)
			return self.f(self.s, self.var)
		end
	})
end

function M:runit(space)
	local waitevery  = self.waitevery
	local printevery = self.printevery
	local migrateby  = self.migrateby
	local index      = self.primary_index

	local pf = function (x,y) return math.floor(x*y)/y end
	local start = fiber.time()
	local c,d,m = 0,0,0
	local size = box.space[space]:len()
	
	-- local pnode = self.pool:prev_leader(space, index, key)
	-- if not pnode or not pnode.conn then
	-- 	log.error('No prev leader conn found for space=%s and key=%s', space, box.tuple.new{key})
	-- 	return false
	-- end
	-- local pconn = pnode.conn
	-- local size = pconn.space[space]:len()

	-- local index_parts = pconn.space[space].index[index].parts

	local index_parts = box.space[space].index[index].parts
	local get_key = function(tuple)
		local key = {}
		for _,part_def in pairs(index_parts) do
			local field_no = part_def.fieldno
			table.insert(key, tuple[field_no])
		end
		return key
	end
	local i  = box.space[space].index[index]
	local it = iter(i, nil, { iterator = box.index.ALL })
	local _, v
	self.tomigrate[space] = {}
	while true do
		c = c + 1
		if c % waitevery == 0 then
			fiber.sleep(0)
			-- reposition iterator after sleep to previous element
			-- beware not to enter here on first step, since we have no v
			it = iter(i, get_key(v), { iterator = box.index.GT })
		end

		_, v = it()
		if v == nil then break end

		if c % printevery == 0 then
			local run = fiber.time() - start
			local rps = c / run
			collectgarbage("collect")
			local mem = collectgarbage("count")
			log.info(
				'Processed %d (%f)%%; migrated %d; in queue %d. Elapsed: %fs (rps: %d) ETA: %fs Mem: %dK',
				c, pf(100 * c / size, 10), m, d,
				pf(run, 1E3), math.floor(c / run), pf(size / rps - run, 1E3), math.floor(mem)
			)
		end

		local key  = get_key(v)
		local pkey = self:packkey(key)

		if self:needmigration(space, index, key, pkey) then
			self.tomigrate[space][pkey] = {1, key}
			d = d + 1
		end

		if d >= migrateby then
			for pk, k in pairs(self.tomigrate[space]) do
				if k[1] == 1 then
					if self:mprocess(space, index, k[2]) then
						m = m + 1
						log.info('Migrated from space[%s]: %s', tostring(space), box.tuple.new{k[2]})
					end
				end
			end
			it = iter(i, key, { iterator = box.index.GT })
			self.tomigrate[space] = {}
			d = 0
		end
	end
	if d > 0 then
		for pk, k in pairs(self.tomigrate[space]) do
			if k[1] == 1 then
				if self:mprocess(space, index, k[2]) then
					m = m + 1
					log.info('Migrated from space[%s]: %s', tostring(space), box.tuple.new{k[2]})
				end
			end
		end
		self.tomigrate[space] = {}
		d = 0
	end

	self.done = true
	self.tomigrate[space] = {}
	self.fiber[space] = nil
	log.info('Done. Processed %d in %fs. Migrated %d tuples', c - 1, pf(fiber.time() - start, 1E3), m)
end


return M
