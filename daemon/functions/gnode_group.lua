#!lua name=gnode_group

--
-- gNode GROUP Functions
-- A ValKey function library for group operations
--
-- This is a port of the gCore Cache Scripts to ValKey functions
-- with enhancements for RESP3 compatibility
-- 

--[[
  - Creating and listing groups
  - Adding and removing group members
  - Setting and getting group properties
  - Managing group permissions and inheritance
  
  All functions use RESP3 compatible responses with proper error handling.
  
  Usage:
  - GNODE_GROUP_LIST(site_id)
  - GNODE_GROUP_CREATE(group_name, settings_json, site_id)
  - GNODE_GROUP_DELETE(group_name, site_id)
  - GNODE_GROUP_ADD_MEMBER(group_name, member_id, site_id)
  - GNODE_GROUP_REMOVE_MEMBER(group_name, member_id, site_id)
  - GNODE_GROUP_GET_MEMBERS(group_name, site_id)
  - GNODE_GROUP_IS_MEMBER(group_name, member_id, site_id)
  - GNODE_GROUP_SET_PROPERTY(group_name, property, value, site_id)
  - GNODE_GROUP_GET_PROPERTY(group_name, property, site_id)
]]

-- Function to track metrics
local function track_metric(site_id, metric_type, value, details)
    -- Skip if site_id not provided
    if not site_id or site_id == "" then
        return
    end
    
    -- Default to increment by 1
    value = value or 1
    
    -- Build metrics key with site isolation
    local metrics_key = '{' .. site_id .. '}:metrics'
    
    -- Track the metric
    server.call('HINCRBY', metrics_key, metric_type, value)
    
    -- Track details if provided
    if details then
        -- Convert details to JSON
        local ok, details_json = pcall(function()
            return cjson.encode(details)
        end)
        
        if ok and details_json then
            -- Store in detail log with timestamp
            local details_key = metrics_key .. ':' .. metric_type .. ':details'
            server.call('LPUSH', details_key, details_json)
            server.call('LTRIM', details_key, 0, 999)  -- Keep last 1000 entries
        end
    end
end

-- Helper function to encode JSON
local function safe_json_encode(value)
    local ok, result = pcall(function()
        return cjson.encode(value)
    end)
    
    if not ok then
        return nil, "JSON encoding failed: " .. tostring(result)
    end
    
    return result
end

-- Helper function to decode JSON
local function safe_json_decode(json_str)
    if not json_str then
        return nil, "JSON string is nil"
    end
    
    local ok, result = pcall(function()
        return cjson.decode(json_str)
    end)
    
    if not ok then
        return nil, "JSON decoding failed: " .. tostring(result)
    end
    
    return result
end

-- Register group list function (mirroring CacheScriptsGroupManager::GROUP_LIST)
server.register_function{
    function_name = 'GNODE_GROUP_LIST',
    callback = function(keys, args)
        -- Input validation
        if not args[1] then
            return server.error_reply("Site ID required")
        end
        
        local site_id = args[1]
        local filter = args[2]
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        
        -- Build group registry key in site's slot
        local registry_key = '{' .. site_id .. '}:groups'
        
        -- Get all groups with their settings
        local groups = server.call('HGETALL', registry_key)
        
        -- Build RESP3-compatible response
        local result = { map = {} }
        for i = 1, #groups, 2 do
            local group_name = groups[i]
            
            -- Apply filter if provided
            if not filter or group_name:match(filter) then
                -- Parse settings
                local ok, settings = pcall(function()
                    return cjson.decode(groups[i + 1])
                end)
                
                if ok and settings then
                    result.map[group_name] = { map = settings }
                else
                    -- Include raw settings if parsing fails
                    result.map[group_name] = {
                        verbatim_string = {
                            format = "txt",
                            string = groups[i + 1]
                        }
                    }
                end
            end
        end
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        track_metric(site_id, 'group_listings', 1, {
            count = #groups / 2,
            filtered = filter ~= nil,
            latency = latency
        })
        
        -- Add metadata to response
        local response = {
            map = {
                groups = result,
                count = #groups / 2,
                site_id = site_id,
                timestamp = { double = start_time / 1000000 }
            }
        }
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    -- Note: No no-writes flag because track_metric writes to metrics hash
    description = 'Lists groups with RESP3 map return'
}

-- Register group create function
server.register_function{
    function_name = 'GNODE_GROUP_CREATE',
    callback = function(keys, args)
        -- Input validation
        if #args < 3 then
            return server.error_reply("Missing required arguments")
        end
        
        local group_name = args[1]
        local settings_json = args[2]
        local site_id = args[3]
        
        -- Validate group name
        if not group_name or group_name == "" then
            return server.error_reply("Invalid group name")
        end
        
        -- Parse settings
        local settings, err = safe_json_decode(settings_json)
        if not settings then
            return server.error_reply("Invalid settings JSON: " .. (err or "unknown"))
        end
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        
        -- Build group registry key in site's slot
        local registry_key = '{' .. site_id .. '}:groups'
        local group_key = '{' .. site_id .. '}:group:' .. group_name
        
        -- Check if group already exists
        if server.call('HEXISTS', registry_key, group_name) == 1 then
            return server.error_reply("Group already exists: " .. group_name)
        end
        
        -- Add default settings if not provided
        settings.created_at = settings.created_at or server.call('TIME')[1]
        settings.updated_at = settings.updated_at or settings.created_at
        settings.member_count = settings.member_count or 0
        
        -- Create group in registry
        local encoded_settings, err = safe_json_encode(settings)
        if not encoded_settings then
            return server.error_reply("Failed to encode settings: " .. (err or "unknown"))
        end
        
        server.call('HSET', registry_key, group_name, encoded_settings)
        
        -- Create members set
        server.call('SADD', group_key .. ':members', '')
        server.call('SREM', group_key .. ':members', '')
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        track_metric(site_id, 'groups_created', 1, {
            group = group_name,
            latency = latency
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                group = group_name,
                site_id = site_id,
                success = true,
                settings = { map = settings },
                timestamp = { double = start_time / 1000000 }
            }
        }
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    description = 'Creates a group with specified settings'
}

-- Register group delete function
server.register_function{
    function_name = 'GNODE_GROUP_DELETE',
    callback = function(keys, args)
        -- Input validation
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local group_name = args[1]
        local site_id = args[2]
        
        -- Validate group name
        if not group_name or group_name == "" then
            return server.error_reply("Invalid group name")
        end
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        
        -- Build group registry key in site's slot
        local registry_key = '{' .. site_id .. '}:groups'
        local group_key = '{' .. site_id .. '}:group:' .. group_name
        
        -- Check if group exists
        if server.call('HEXISTS', registry_key, group_name) == 0 then
            return server.error_reply("Group not found: " .. group_name)
        end
        
        -- Get current settings
        local settings_json = server.call('HGET', registry_key, group_name)
        local settings, _ = safe_json_decode(settings_json)
        
        -- Delete group from registry
        server.call('HDEL', registry_key, group_name)
        
        -- Delete members set and other group keys
        server.call('DEL', group_key .. ':members')
        server.call('DEL', group_key .. ':properties')
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        track_metric(site_id, 'groups_deleted', 1, {
            group = group_name,
            latency = latency
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                group = group_name,
                site_id = site_id,
                success = true,
                settings = settings and { map = settings } or nil,
                timestamp = { double = start_time / 1000000 }
            }
        }
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    description = 'Deletes a group and all its related keys'
}

-- Register add member function
server.register_function{
    function_name = 'GNODE_GROUP_ADD_MEMBER',
    callback = function(keys, args)
        -- Input validation
        if #args < 3 then
            return server.error_reply("Missing required arguments")
        end
        
        local group_name = args[1]
        local member_id = args[2]
        local site_id = args[3]
        
        -- Validate inputs
        if not group_name or group_name == "" then
            return server.error_reply("Invalid group name")
        end
        
        if not member_id or member_id == "" then
            return server.error_reply("Invalid member ID")
        end
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        
        -- Build group registry key in site's slot
        local registry_key = '{' .. site_id .. '}:groups'
        local group_key = '{' .. site_id .. '}:group:' .. group_name
        
        -- Check if group exists
        if server.call('HEXISTS', registry_key, group_name) == 0 then
            return server.error_reply("Group not found: " .. group_name)
        end
        
        -- Add member to group
        local added = server.call('SADD', group_key .. ':members', member_id)
        
        -- Update member count in settings if member was added
        if added == 1 then
            local settings_json = server.call('HGET', registry_key, group_name)
            local settings, _ = safe_json_decode(settings_json)
            
            if settings then
                settings.member_count = (settings.member_count or 0) + 1
                settings.updated_at = server.call('TIME')[1]
                
                local encoded_settings, _ = safe_json_encode(settings)
                if encoded_settings then
                    server.call('HSET', registry_key, group_name, encoded_settings)
                end
            end
        end
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        track_metric(site_id, 'group_members_added', 1, {
            group = group_name,
            member = member_id,
            success = added == 1,
            latency = latency
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                group = group_name,
                member = member_id,
                site_id = site_id,
                added = added == 1,
                timestamp = { double = start_time / 1000000 }
            }
        }
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    description = 'Adds a member to a group'
}

-- Register remove member function
server.register_function{
    function_name = 'GNODE_GROUP_REMOVE_MEMBER',
    callback = function(keys, args)
        -- Input validation
        if #args < 3 then
            return server.error_reply("Missing required arguments")
        end
        
        local group_name = args[1]
        local member_id = args[2]
        local site_id = args[3]
        
        -- Validate inputs
        if not group_name or group_name == "" then
            return server.error_reply("Invalid group name")
        end
        
        if not member_id or member_id == "" then
            return server.error_reply("Invalid member ID")
        end
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        
        -- Build group registry key in site's slot
        local registry_key = '{' .. site_id .. '}:groups'
        local group_key = '{' .. site_id .. '}:group:' .. group_name
        
        -- Check if group exists
        if server.call('HEXISTS', registry_key, group_name) == 0 then
            return server.error_reply("Group not found: " .. group_name)
        end
        
        -- Remove member from group
        local removed = server.call('SREM', group_key .. ':members', member_id)
        
        -- Update member count in settings if member was removed
        if removed == 1 then
            local settings_json = server.call('HGET', registry_key, group_name)
            local settings, _ = safe_json_decode(settings_json)
            
            if settings then
                settings.member_count = math.max(0, (settings.member_count or 0) - 1)
                settings.updated_at = server.call('TIME')[1]
                
                local encoded_settings, _ = safe_json_encode(settings)
                if encoded_settings then
                    server.call('HSET', registry_key, group_name, encoded_settings)
                end
            end
        end
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        track_metric(site_id, 'group_members_removed', 1, {
            group = group_name,
            member = member_id,
            success = removed == 1,
            latency = latency
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                group = group_name,
                member = member_id,
                site_id = site_id,
                removed = removed == 1,
                timestamp = { double = start_time / 1000000 }
            }
        }
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    description = 'Removes a member from a group'
}

-- Register get members function
server.register_function{
    function_name = 'GNODE_GROUP_GET_MEMBERS',
    callback = function(keys, args)
        -- Input validation
        if #args < 2 then
            return server.error_reply("Missing required arguments")
        end
        
        local group_name = args[1]
        local site_id = args[2]
        
        -- Validate inputs
        if not group_name or group_name == "" then
            return server.error_reply("Invalid group name")
        end
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        
        -- Build group registry key in site's slot
        local registry_key = '{' .. site_id .. '}:groups'
        local group_key = '{' .. site_id .. '}:group:' .. group_name
        
        -- Check if group exists
        if server.call('HEXISTS', registry_key, group_name) == 0 then
            return server.error_reply("Group not found: " .. group_name)
        end
        
        -- Get members
        local members = server.call('SMEMBERS', group_key .. ':members')
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        track_metric(site_id, 'group_members_listed', 1, {
            group = group_name,
            count = #members,
            latency = latency
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                group = group_name,
                site_id = site_id,
                members = { set = {} },
                count = #members,
                timestamp = { double = start_time / 1000000 }
            }
        }
        
        -- Add members to response
        for _, member in ipairs(members) do
            response.map.members.set[member] = true
        end
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    -- Note: No no-writes flag because track_metric writes to metrics hash
    description = 'Gets all members of a group'
}

-- Register is member function
server.register_function{
    function_name = 'GNODE_GROUP_IS_MEMBER',
    callback = function(keys, args)
        -- Input validation
        if #args < 3 then
            return server.error_reply("Missing required arguments")
        end
        
        local group_name = args[1]
        local member_id = args[2]
        local site_id = args[3]
        
        -- Validate inputs
        if not group_name or group_name == "" then
            return server.error_reply("Invalid group name")
        end
        
        if not member_id or member_id == "" then
            return server.error_reply("Invalid member ID")
        end
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        
        -- Build group registry key in site's slot
        local registry_key = '{' .. site_id .. '}:groups'
        local group_key = '{' .. site_id .. '}:group:' .. group_name
        
        -- Check if group exists
        if server.call('HEXISTS', registry_key, group_name) == 0 then
            return server.error_reply("Group not found: " .. group_name)
        end
        
        -- Check membership
        local is_member = server.call('SISMEMBER', group_key .. ':members', member_id) == 1
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        track_metric(site_id, 'group_membership_checks', 1, {
            group = group_name,
            member = member_id,
            is_member = is_member,
            latency = latency
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                group = group_name,
                member = member_id,
                site_id = site_id,
                is_member = is_member,
                timestamp = { double = start_time / 1000000 }
            }
        }
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    -- Note: No no-writes flag because track_metric writes to metrics hash
    description = 'Checks if a member belongs to a group'
}

-- Register set property function
server.register_function{
    function_name = 'GNODE_GROUP_SET_PROPERTY',
    callback = function(keys, args)
        -- Input validation
        if #args < 4 then
            return server.error_reply("Missing required arguments")
        end
        
        local group_name = args[1]
        local property = args[2]
        local value = args[3]
        local site_id = args[4]
        
        -- Validate inputs
        if not group_name or group_name == "" then
            return server.error_reply("Invalid group name")
        end
        
        if not property or property == "" then
            return server.error_reply("Invalid property name")
        end
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        
        -- Build group registry key in site's slot
        local registry_key = '{' .. site_id .. '}:groups'
        local group_key = '{' .. site_id .. '}:group:' .. group_name
        
        -- Check if group exists
        if server.call('HEXISTS', registry_key, group_name) == 0 then
            return server.error_reply("Group not found: " .. group_name)
        end
        
        -- Set property
        server.call('HSET', group_key .. ':properties', property, value)
        
        -- Update settings to mark group as updated
        local settings_json = server.call('HGET', registry_key, group_name)
        local settings, _ = safe_json_decode(settings_json)
        
        if settings then
            settings.updated_at = server.call('TIME')[1]
            
            local encoded_settings, _ = safe_json_encode(settings)
            if encoded_settings then
                server.call('HSET', registry_key, group_name, encoded_settings)
            end
        end
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        track_metric(site_id, 'group_properties_set', 1, {
            group = group_name,
            property = property,
            latency = latency
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                group = group_name,
                property = property,
                value = value,
                site_id = site_id,
                success = true,
                timestamp = { double = start_time / 1000000 }
            }
        }
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    description = 'Sets a property for a group'
}

-- Register get property function
server.register_function{
    function_name = 'GNODE_GROUP_GET_PROPERTY',
    callback = function(keys, args)
        -- Input validation
        if #args < 3 then
            return server.error_reply("Missing required arguments")
        end
        
        local group_name = args[1]
        local property = args[2]
        local site_id = args[3]
        
        -- Validate inputs
        if not group_name or group_name == "" then
            return server.error_reply("Invalid group name")
        end
        
        if not property or property == "" then
            return server.error_reply("Invalid property name")
        end
        
        -- Track operation timing
        local start_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        
        -- Build group registry key in site's slot
        local registry_key = '{' .. site_id .. '}:groups'
        local group_key = '{' .. site_id .. '}:group:' .. group_name
        
        -- Check if group exists
        if server.call('HEXISTS', registry_key, group_name) == 0 then
            return server.error_reply("Group not found: " .. group_name)
        end
        
        -- Get property
        local value = server.call('HGET', group_key .. ':properties', property)
        
        -- Track metrics
        local end_time = server.call('TIME')[1] * 1000000 + server.call('TIME')[2]
        local latency = end_time - start_time
        
        track_metric(site_id, 'group_properties_get', 1, {
            group = group_name,
            property = property,
            found = value ~= nil,
            latency = latency
        })
        
        -- Build RESP3-compatible response
        local response = {
            map = {
                group = group_name,
                property = property,
                value = value,
                site_id = site_id,
                found = value ~= nil,
                timestamp = { double = start_time / 1000000 }
            }
        }
        
        -- Convert to RESP3 or JSON depending on server capability
        if server.response_version and server.response_version() >= 3 then
            return response
        else
            local json, err = safe_json_encode(response)
            if not json then
                return server.error_reply("JSON encoding error: " .. (err or "unknown"))
            end
            return json
        end
    end,
    -- Note: No no-writes flag because track_metric writes to metrics hash
    description = 'Gets a property from a group'
}