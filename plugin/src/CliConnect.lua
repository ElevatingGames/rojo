--[[
	Lets `rojo connect` start a sync session from the command line, as if the
	Connect button had been clicked.

	While the plugin is idle, it long-polls the Rojo server for connect requests.
	When it claims one, it connects as usual and reports its progress back to the
	server, where the CLI follows along. If the connection needs confirmation,
	the changes are sent along so the CLI can list them, and the CLI's answer is
	waited for alongside the confirmation page in Studio. Whichever is answered
	first wins.
]]

local Packages = script.Parent.Parent.Packages
local Http = require(Packages.Http)
local Log = require(Packages.Log)

-- The CLI only lists a few changes, so there's no need to send every one.
local MAX_ENTRIES = 200

-- How long to wait before asking again after failing to reach the server
-- while waiting for the CLI's decision.
local DECISION_RETRY_DELAY = 2

local KIND_ORDER = {
	remove = 1,
	add = 2,
	update = 3,
}

local CliConnect = {}

--[[
	Waits for a connect request to claim from the server at `baseUrl`.
	Resolves with the claimed request, or nil if there wasn't one before the
	server's poll timed out.
]]
function CliConnect.poll(baseUrl: string)
	local body = Http.msgpackEncode({
		-- Sent as a string, since msgpack would turn large place IDs into floats.
		placeId = string.format("%d", game.PlaceId),
		placeName = game.Name,
	})

	return Http.post(baseUrl .. "/api/connect/poll", body):andThen(Http.Response.msgpack):andThen(function(response)
		return response.request
	end)
end

--[[
	Returns whether an error from `CliConnect.poll` means there's no server
	running, as opposed to some other problem.
]]
function CliConnect.isConnectFailure(err): boolean
	return type(err) == "table" and err.type == Http.Error.Kind.ConnectFailed
end

local function getFullName(instance: Instance): string
	if instance == game then
		return "game"
	end

	return instance:GetFullName()
end

--[[
	Describes a patch the way the CLI shows it. Additions are listed once for
	each added subtree, at its root.
]]
function CliConnect.summarizePatch(instanceMap, patch)
	local entries = {}
	local added, removed, updated = 0, 0, 0

	for _, idOrInstance in patch.removed do
		local instance = if typeof(idOrInstance) == "Instance" then idOrInstance else instanceMap.fromIds[idOrInstance]

		if instance ~= nil then
			local descendants = #instance:GetDescendants()
			removed += 1 + descendants

			table.insert(entries, {
				kind = "remove",
				path = getFullName(instance),
				className = instance.ClassName,
				descendants = if descendants > 0 then descendants else nil,
			})
		else
			removed += 1

			table.insert(entries, {
				kind = "remove",
				path = string.format("[unknown instance %s]", tostring(idOrInstance)),
			})
		end
	end

	local function countAddedDescendants(virtualInstance): number
		local count = 0
		for _, childId in virtualInstance.Children or {} do
			local child = patch.added[childId]
			if child ~= nil then
				count += 1 + countAddedDescendants(child)
			end
		end
		return count
	end

	for _, virtualInstance in patch.added do
		added += 1

		local parentId = virtualInstance.Parent
		if parentId ~= nil and patch.added[parentId] ~= nil then
			-- This is listed as a descendant of an addition further up.
			continue
		end

		local parent = if parentId ~= nil then instanceMap.fromIds[parentId] else nil
		local path
		if parent == nil then
			path = "[unknown parent]." .. virtualInstance.Name
		elseif parent == game then
			path = virtualInstance.Name
		else
			path = parent:GetFullName() .. "." .. virtualInstance.Name
		end

		local descendants = countAddedDescendants(virtualInstance)

		table.insert(entries, {
			kind = "add",
			path = path,
			className = virtualInstance.ClassName,
			descendants = if descendants > 0 then descendants else nil,
		})
	end

	for _, update in patch.updated do
		updated += 1

		local properties = {}
		if update.changedName ~= nil then
			table.insert(properties, "Name")
		end
		if update.changedClassName ~= nil then
			table.insert(properties, "ClassName")
		end
		for name in update.changedProperties or {} do
			table.insert(properties, name)
		end
		table.sort(properties)

		local instance = instanceMap.fromIds[update.id]

		table.insert(entries, {
			kind = "update",
			path = if instance ~= nil then getFullName(instance) else "[unknown instance]",
			className = if instance ~= nil then instance.ClassName else nil,
			-- Empty tables would be encoded as maps rather than lists.
			properties = if #properties > 0 then properties else nil,
		})
	end

	table.sort(entries, function(a, b)
		if a.path ~= b.path then
			return a.path < b.path
		end
		return KIND_ORDER[a.kind] < KIND_ORDER[b.kind]
	end)

	local totalEntries = #entries
	if totalEntries > MAX_ENTRIES then
		entries = table.move(entries, 1, MAX_ENTRIES, 1, {})
	end

	return {
		added = added,
		removed = removed,
		updated = updated,
		entries = if #entries > 0 then entries else nil,
		totalEntries = totalEntries,
	}
end

--[[
	A connect request this plugin claimed, which it reports its progress on.
]]
local Request = {}
Request.__index = Request

CliConnect.Request = Request

function Request.new(baseUrl: string, claim)
	local self = {
		baseUrl = baseUrl,
		id = claim.requestId,
		force = claim.force == true,
		finished = false,
		-- Set once the confirmation was answered with "Abort", in Studio or
		-- by the CLI.
		declined = false,
		__seq = 0,
	}

	return setmetatable(self, Request)
end

function Request:__url(action: string): string
	return string.format("%s/api/connect/%s/%s", self.baseUrl, self.id, action)
end

function Request:__report(state: string, message: string?, changes)
	if self.finished then
		return nil
	end

	if state == "connected" or state == "aborted" or state == "failed" then
		self.finished = true
	end

	-- Reports are sent concurrently, so number them to let the server ignore
	-- any that arrive out of order.
	self.__seq += 1

	local body = Http.msgpackEncode({
		seq = self.__seq,
		state = state,
		message = message,
		changes = changes,
	})

	return Http.post(self:__url("status"), body):catch(function(err)
		Log.debug("Could not report connection status to 'rojo connect': {}", tostring(err))
	end)
end

function Request:progress(text: string)
	self:__report("connecting", text)
end

function Request:connected(projectName: string?)
	self:__report("connected", projectName)
end

function Request:aborted(reason: string?)
	self:__report("aborted", reason)
end

function Request:failed(message: string)
	self:__report("failed", message)
end

--[[
	Reports the changes awaiting confirmation, then waits for the CLI to accept
	or abort them, calling `onDecision` with "Accept" or "Abort" if it does.

	Returns a function that stops waiting, which must be called once the
	confirmation is answered in any way.
]]
function Request:awaitDecision(instanceMap, patch, onDecision: (string) -> ())
	local waiting = true
	local reported = self:__report("confirming", nil, CliConnect.summarizePatch(instanceMap, patch))

	task.spawn(function()
		-- Asking before the server knows we're confirming would make it look
		-- like there's nothing to wait for.
		if reported ~= nil then
			reported:await()
		end

		while waiting do
			local success, response = Http.get(self:__url("decision")):andThen(Http.Response.msgpack):await()

			if not waiting then
				break
			end

			if not success then
				Log.debug("Could not check for a decision from 'rojo connect': {}", tostring(response))
				task.wait(DECISION_RETRY_DELAY)
			elseif response.decision == "accept" or response.decision == "abort" then
				waiting = false
				onDecision(if response.decision == "accept" then "Accept" else "Abort")
			elseif response.state ~= "confirming" then
				-- The CLI isn't going to decide, so leave it to Studio.
				waiting = false
			end
		end
	end)

	return function()
		waiting = false
	end
end

return CliConnect
