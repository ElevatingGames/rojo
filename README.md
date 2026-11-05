<div align="center">
    <a href="https://rojo.space"><img src="assets/brand_images/logo-512.png" alt="Rojo" height="217" /></a>
</div>

<div>&nbsp;</div>

<div align="center">
    <a href="https://github.com/rojo-rbx/rojo/actions"><img src="https://github.com/rojo-rbx/rojo/workflows/CI/badge.svg" alt="Actions status" /></a>
    <a href="https://crates.io/crates/rojo"><img src="https://img.shields.io/crates/v/rojo.svg?label=latest%20release" alt="Latest server version" /></a>
    <a href="https://rojo.space/docs"><img src="https://img.shields.io/badge/docs-website-brightgreen.svg" alt="Rojo Documentation" /></a>
</div>

<hr />

**Rojo** is a tool designed to enable Roblox developers to use professional-grade software engineering tools.

With Rojo, it's possible to use industry-leading tools like **Visual Studio Code** and **Git**.

Rojo is designed for power users who want to use the best tools available for building games, libraries, and plugins.

## Features
Rojo enables:

* Working on scripts and models from the filesystem, in your favorite editor
* Versioning your game, library, or plugin using Git or another VCS
* Streaming `rbxmx` and `rbxm` models into your game in real time
* Packaging and deploying your project to Roblox.com from the command line
* Pulling Instances from Roblox place and model files back into an existing Rojo project with `rojo syncback`

Rojo also has an optional two-way sync setting in the Studio plugin for syncing supported Studio edits back to the filesystem.

Some workflows, like fully automatic conversion of every existing game into a Rojo project, are still limited and may require manual project configuration.

## Connecting Studio from the command line
This fork adds `rojo connect`, which makes the Studio plugin connect to a running `rojo serve`, just like clicking **Connect** in Studio:

```sh
rojo serve            # in one terminal
rojo connect          # in another, or from a script or editor task
```

`rojo connect` reads the address and port from the project in the current directory (or the project path you give it), falling back to `localhost:34872`. The Studio plugin's address and port must point at the same server, as they do for the Connect button.

When connecting would change Studio and the plugin would normally ask for confirmation (see the plugin's *Confirmation Behavior* setting), `rojo connect` lists the changes and asks whether to continue. You can answer in the terminal or in Studio. If there is no terminal to ask in, `rojo connect` does not connect and exits with an error.

| Option | Description |
|---|---|
| `-f`, `--force` | Connect without asking, overwriting any changes in Studio. |
| `--place-id <ID>` | Only connect a Studio window that has this place open. |
| `--timeout <SECONDS>` | How long to wait for Studio to pick up the request (default 30). |
| `--address`, `--port` | Override the server address and port. |

This needs the plugin from this build, installed with `rojo plugin install`. While disconnected, the plugin checks the server for `rojo connect` requests. You can turn that off with the plugin's *Allow CLI Connect* setting. Only clients on the same machine as the server can make requests.

## [Documentation](https://rojo.space/docs)
Documentation is hosted in the [rojo.space repository](https://github.com/rojo-rbx/rojo.space).

## Contributing
Check out our [contribution guide](CONTRIBUTING.md) for detailed instructions for helping work on Rojo!

Pull requests are welcome!

Rojo supports Rust 1.88 and newer. The minimum supported version of Rust is based on the latest versions of the dependencies that Rojo has.

## License
Rojo is available under the terms of the Mozilla Public License, Version 2.0. See [LICENSE.txt](LICENSE.txt) for details.
