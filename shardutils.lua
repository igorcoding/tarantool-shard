local bh = require('binaryheap')
local fiber = require('fiber')
local log = require('log')
local yaml = require('yaml')

local obj = require('obj')

local utils = {}

function utils.error(msg, ...)
	local args = {...}
	if #args > 0 then
		msg = string.format(msg, ...)
	end
	box.error({reason = tostring(msg)})
end

function utils.get_space(space)
	local space_def = box.space[space]
	if space_def == nil then
		log.error("Couldn't find space '%s'", space)
		return nil
	end
	return space_def
end

function utils.get_index(space, index)
	local space_def = utils.get_space(space)
	if space_def == nil then return nil end
	
	local index_def = space_def.index[index]
	if index_def == nil then
		log.error("Couldn't find index '%s' for space '%s'", index, space)
		return nil
	end
	return index_def
end

function utils.extract_key_by_index(space, index, tuple)
	local index_def = utils.get_index(space, index)
	if index_def == nil then return nil end
		
	local key = {}
	
	local index_parts = index_def.parts
	for _,part_def in pairs(index_parts) do
		local field_no = part_def.fieldno
		table.insert(key, tuple[field_no])
	end
	return key
end

function utils.is_index_unique(space, index)
	local index_def = utils.get_index(space, index)
	if index_def == nil then return nil end
	
	if index_def.unique == nil then
		return true
	end
	return index_def.unique
end

function utils.index_parts_count(space, index)
	local index_def = utils.get_index(space, index)
	if index_def == nil then return nil end
	
	if index_def.parts == nil then
		return 0
	end
	return #index_def.parts
end

function utils.tuples_empty(tuples)
	return not tuples or #tuples == 0
end

function utils.merge_tuples(results, space, index, ...)
	-- merge results according to space and index.
	-- first idea. Create temporary space with required index, insert all data there, select from it and delete space in the end.
	-- second idea. Implement a heap, create heap from entire dataset in O(n1 + n2 + ... nk)
	-- third idea. Merge sort using heap, but merging is complicated by the fact that index has an arbitrary structure.
	
	-- What if index is of HASH type? Still need to merge somehow, but don't have access to hash function
	local args = {...}
	local iterator
	if #args == 0 then
		iterator = box.index.EQ
	else
		opts = args[ #args ]
		if opts == nil or type(opts) ~= 'table' or opts.iterator == nil then
			iterator = box.index.EQ
		else
			iterator = opts.iterator
			if iterator == 'EQ' or iterator == box.index.EQ then
				iterator = box.index.EQ
			elseif iterator == 'REQ' or iterator == box.index.REQ then
				iterator = box.index.REQ
			elseif iterator == 'GT' or iterator == box.index.GT then
				iterator = box.index.GT
			elseif iterator == 'GE' or iterator == box.index.GE then
				iterator = box.index.GE
			elseif iterator == 'ALL' or iterator == box.index.ALL then
				iterator = box.index.ALL
			elseif iterator == 'LT' or iterator == box.index.LT then
				iterator = box.index.LT
			elseif iterator == 'LE' or iterator == box.index.LE then
				iterator = box.index.LE
			elseif iterator == 'BITS_ALL_SET' or iterator == box.index.BITS_ALL_SET then
				utils.error('%s iterator not supported', iterator)
				-- iterator = box.index.BITS_ALL_SET
			elseif iterator == 'BITS_ANY_SET' or iterator == box.index.BITS_ANY_SET then
				utils.error('%s iterator not supported', iterator)
				-- iterator = box.index.BITS_ANY_SET
			elseif iterator == 'BITS_ALL_NOT_SET' or iterator == box.index.BITS_ALL_NOT_SET then
				utils.error('%s iterator not supported', iterator)
				-- iterator = box.index.BITS_ALL_NOT_SET
			elseif iterator == 'OVERLAPS' or iterator == box.index.OVERLAPS then
				utils.error('%s iterator not supported', iterator)
				-- iterator = box.index.OVERLAPS
			elseif iterator == 'NEIGHBOR' or iterator == box.index.NEIGHBOR then
				utils.error('%s iterator not supported', iterator)
				-- iterator = box.index.NEIGHBOR
			else
				iterator = box.index.EQ
			end
		end
	end
	
	local heap
	if iterator == box.index.EQ
		or iterator == box.index.GE
		or iterator == box.index.GT
		or iterator == box.index.ALL then
		
		local comparator = function(a, b)
			local max_fields_count = math.max(#a, #b)
			for f = 1, max_fields_count do
				if f <= #a and f <= #b then
					if a[f] < b[f] then
						return true
					elseif a[f] > b[f] then
						return false
					else
						-- continue
					end
				else
					if f > #a then
						return true
					elseif f > #b then
						return false
					else
						return true
					end
				end
			end
			return true
		end
		heap = bh.minUnique(comparator)
	else
		local comparator = function(a, b)
			local max_fields_count = math.max(#a, #b)
			for f = 1, max_fields_count do
				if f <= #a and f <= #b then
					if a[f] > b[f] then
						return true
					elseif a[f] < b[f] then
						return false
					else
						-- continue
					end
				else
					if f > #a then
						return false
					elseif f > #b then
						return true
					else
						return false
					end
				end
			end
			return false
		end
		heap = bh.maxUnique(comparator)
	end
	
	
	local indexes = {}
	local last_k = nil
	
	-- initial fill
	for k,tuples in pairs(results) do
		if #tuples > 0 then
			indexes[k] = 1
			local tuple = tuples[ indexes[k] ]
			indexes[k] = indexes[k] + 1
			local key = utils.extract_key_by_index(space, index, tuple)
			if key ~= nil then
				heap:insert(key, {k, tuple})
			end
		end
	end
	
	local merged = {}
	while #heap.values > 0 do
		local _, payload = heap:pop()
		local k, tuple = unpack(payload)
		table.insert(merged, tuple)
		
		local tuples = results[k]
		if #tuples > 0 and indexes[k] <= #tuples then
			local tuple = tuples[ indexes[k] ]
			indexes[k] = indexes[k] + 1
			local key = utils.extract_key_by_index(space, index, tuple)
			if key ~= nil then
				heap:insert(key, {k, tuple})
			end
		end
	end
	
	return merged
end


local Hooker = obj.class({
	hooks = {},
	__call = function(self, separate_fiber)
		local hooks = self.hooks
		log.info('Hooker call on %d functions', #hooks)
		separate_fiber = separate_fiber or false
		for _,h in ipairs(hooks) do
			f, args = unpack(h)
			if separate_fiber then
				fiber.create(f, unpack(args))
			else
				f(unpack(args))
			end
		end
	end
}, 'Hooker')

function Hooker:add(f, ...)
	self.hooks[#self.hooks + 1] = {f, {...}}
end

function Hooker:clean()
	self.hooks = {}
end

utils.Hooker = Hooker


return utils
