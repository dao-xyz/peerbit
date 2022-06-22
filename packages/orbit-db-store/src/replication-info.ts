export class ReplicationInfo {
  progress: number;
  max: number;
  constructor() {
    this.progress = 0
    this.max = 0
  }

  reset() {
    this.progress = 0
    this.max = 0
  }
}

