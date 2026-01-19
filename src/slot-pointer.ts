import { Slot } from './slot';

export class SlotPointer {
  readonly position: bigint | null;
  readonly slot: Slot;

  constructor(position: bigint | null, slot: Slot) {
    this.position = position;
    this.slot = slot;
  }

  withSlot(slot: Slot): SlotPointer {
    return new SlotPointer(this.position, slot);
  }
}
