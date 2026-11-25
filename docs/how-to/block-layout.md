# Set the web GUI block layout

The IOC can read and change where each block appears in the PandA's web GUI, through the Malcolm server the GUI uses (port 8008 by default, see `--malcolm-port`). Each block has:

- `<BLOCK>:LAYOUT:X` and `<BLOCK>:LAYOUT:Y`, the block's position in the GUI.
- `<BLOCK>:LAYOUT:VISIBLE`, whether the block is shown (`Visible`) or hidden (`Hidden`).

These follow changes made in the web GUI, and writing them moves the block in the GUI.

:::{warning}
Hiding a block is not only a display change. Malcolm disconnects the inputs of a hidden block, setting them back to `ZERO`, and resets its position to 0, 0. Only hide blocks that are not in use.
:::

Malcolm only stores the position of visible blocks. A position written to a hidden block is reset to 0, 0 the next time any block moves, so to show a block in a particular place, write its `X`, `Y` and `VISIBLE` together.

Changes written within 0.1 s of each other, such as `X` then `Y` from a script, are sent to Malcolm together. Malcolm stores the layout on the PandA and reads it back afterwards, and a change made while it is doing so can be reverted. So the IOC waits about a second after each change before sending the next, and sends a change again if Malcolm reverted it. Writes can therefore take a second or two to show in the GUI.

If Malcolm can't be reached, the layout PVs are marked invalid, and are filled in once it can be. Use `--no-layout` to not create the layout PVs at all.
