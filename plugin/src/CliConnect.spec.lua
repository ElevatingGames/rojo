return function()
	local CliConnect = require(script.Parent.CliConnect)

	local InstanceMap = require(script.Parent.InstanceMap)
	local PatchSet = require(script.Parent.PatchSet)

	local root, child, grandchild
	local instanceMap

	beforeEach(function()
		root = Instance.new("Folder")
		root.Name = "Root"

		child = Instance.new("Folder")
		child.Name = "Child"
		child.Parent = root

		grandchild = Instance.new("Part")
		grandchild.Name = "Grandchild"
		grandchild.Parent = child

		instanceMap = InstanceMap.new()
		instanceMap:insert("ROOT", root)
		instanceMap:insert("CHILD", child)
	end)

	afterEach(function()
		instanceMap:stop()
		root:Destroy()
	end)

	it("should summarize an empty patch", function()
		local summary = CliConnect.summarizePatch(instanceMap, PatchSet.newEmpty())

		expect(summary.added).to.equal(0)
		expect(summary.removed).to.equal(0)
		expect(summary.updated).to.equal(0)
		expect(summary.totalEntries).to.equal(0)
		-- An empty table would be sent as a map rather than a list.
		expect(summary.entries).to.equal(nil)
	end)

	it("should list a removal once, counting its descendants", function()
		local patch = PatchSet.newEmpty()
		table.insert(patch.removed, child)

		local summary = CliConnect.summarizePatch(instanceMap, patch)

		expect(summary.removed).to.equal(2)
		expect(#summary.entries).to.equal(1)

		local entry = summary.entries[1]
		expect(entry.kind).to.equal("remove")
		expect(entry.path).to.equal("Root.Child")
		expect(entry.className).to.equal("Folder")
		expect(entry.descendants).to.equal(1)
	end)

	it("should find removals given by ID", function()
		local patch = PatchSet.newEmpty()
		table.insert(patch.removed, "CHILD")
		table.insert(patch.removed, "MISSING")

		local summary = CliConnect.summarizePatch(instanceMap, patch)

		expect(summary.removed).to.equal(3)
		expect(summary.entries[1].path).to.equal("Root.Child")
		expect(summary.entries[2].path).to.equal("[unknown instance MISSING]")
	end)

	it("should list additions at the top of each added subtree", function()
		local patch = PatchSet.newEmpty()
		patch.added["A"] = {
			Name = "A",
			ClassName = "Folder",
			Parent = "ROOT",
			Properties = {},
			Children = { "B" },
		}
		patch.added["B"] = {
			Name = "B",
			ClassName = "Folder",
			Parent = "A",
			Properties = {},
			Children = { "C" },
		}
		patch.added["C"] = {
			Name = "C",
			ClassName = "Part",
			Parent = "B",
			Properties = {},
			Children = {},
		}

		local summary = CliConnect.summarizePatch(instanceMap, patch)

		expect(summary.added).to.equal(3)
		expect(#summary.entries).to.equal(1)

		local entry = summary.entries[1]
		expect(entry.kind).to.equal("add")
		expect(entry.path).to.equal("Root.A")
		expect(entry.className).to.equal("Folder")
		expect(entry.descendants).to.equal(2)
	end)

	it("should list the properties an update changes", function()
		local patch = PatchSet.newEmpty()
		table.insert(patch.updated, {
			id = "CHILD",
			changedName = "Renamed",
			changedProperties = {
				Tags = {},
				Attributes = {},
			},
		})

		local summary = CliConnect.summarizePatch(instanceMap, patch)

		expect(summary.updated).to.equal(1)

		local entry = summary.entries[1]
		expect(entry.kind).to.equal("update")
		expect(entry.path).to.equal("Root.Child")
		expect(entry.className).to.equal("Folder")
		expect(#entry.properties).to.equal(3)
		expect(entry.properties[1]).to.equal("Attributes")
		expect(entry.properties[2]).to.equal("Name")
		expect(entry.properties[3]).to.equal("Tags")
	end)

	it("should leave out the properties of an update that changes none", function()
		local patch = PatchSet.newEmpty()
		table.insert(patch.updated, {
			id = "MISSING",
			changedProperties = {},
		})

		local summary = CliConnect.summarizePatch(instanceMap, patch)

		local entry = summary.entries[1]
		expect(entry.path).to.equal("[unknown instance]")
		-- An empty table would be sent as a map rather than a list.
		expect(entry.properties).to.equal(nil)
	end)

	it("should sort entries by path, with removals first", function()
		local patch = PatchSet.newEmpty()
		table.insert(patch.removed, child)
		patch.added["NEW_CHILD"] = {
			Name = "Child",
			ClassName = "Folder",
			Parent = "ROOT",
			Properties = {},
			Children = {},
		}
		patch.added["A"] = {
			Name = "A",
			ClassName = "Folder",
			Parent = "ROOT",
			Properties = {},
			Children = {},
		}

		local summary = CliConnect.summarizePatch(instanceMap, patch)

		expect(#summary.entries).to.equal(3)
		expect(summary.entries[1].path).to.equal("Root.A")
		expect(summary.entries[2].path).to.equal("Root.Child")
		expect(summary.entries[2].kind).to.equal("remove")
		expect(summary.entries[3].path).to.equal("Root.Child")
		expect(summary.entries[3].kind).to.equal("add")
	end)

	it("should truncate long lists but report how many entries there were", function()
		local patch = PatchSet.newEmpty()
		for i = 1, 250 do
			table.insert(patch.removed, string.format("MISSING_%03d", i))
		end

		local summary = CliConnect.summarizePatch(instanceMap, patch)

		expect(summary.removed).to.equal(250)
		expect(summary.totalEntries).to.equal(250)
		expect(#summary.entries).to.equal(200)
	end)
end
