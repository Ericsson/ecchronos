# Commands

Standalone ecChronos includes a commandline utlity to check repair status, repair config, start/stop ecChronos service as well as trigger a single repair.

## Subcommands

The following subcommands are available:

| Command                                                   | Description                           |
|-----------------------------------------------------------|---------------------------------------|
| `ecc repair-status`                                       | Repair status overview                |
| `ecc repair-config`                                       | Configuration overview                |
| `ecc start`                                               | Start ecChronos service               |
| `ecc stop  `                                              | Stop ecChronos service                |
| `ecc trigger-repair`                                      | Trigger a single repair               |

### Flags

Each of the subcommands support flags. For more information provide `-h` flag after subcommand.
For example:

```
ecc repair-status -h
```