export interface SerialQueue {
  run<Result>(operation: () => Promise<Result>): Promise<Result>;
}

export function createSerialQueue(): SerialQueue {
  let queue: Promise<void> = Promise.resolve();

  return {
    async run<Result>(operation: () => Promise<Result>): Promise<Result> {
      const previous = queue;
      let release: () => void = () => undefined;

      queue = new Promise<void>((resolve) => {
        release = resolve;
      });

      await previous;
      try {
        return await operation();
      } finally {
        release();
      }
    },
  };
}
