import hope from 'hoper';

export function isError(obj) {
  return !!Object.prototype.toString.call(obj).match(/object \d*Error/);
}

export function isDate(obj) {
  return Object.prototype.toString.call(obj) === '[object Date]';
}

export function isRegExp(obj) {
  return Object.prototype.toString.call(obj) === '[object RegExp]';
}

export function promiseWaterfall<T, CB extends (payload: T) => Promise<T>>(callbacks, initialArgs?: T): Promise<T> {
  // Don't assume we're running in an environment with promises
  return callbacks.reduce(function (accumulator, callback) {
    return accumulator.then(callback);
  }, Promise.resolve(initialArgs));
}

export function maybePromise(self: { sync: boolean }, args: any[]) {
  const { sync } = self;

  function handler(executor: Function) {
    const last = args[args.length - 1];
    const callback = typeof last === 'function' ? last : undefined;
    const argsWithoutCb = callback ? args.slice(0, -1) : args;

    if (sync) {
      let payload;

      if (callback) {
        executor([
          ...argsWithoutCb,
          (err, res) => {
            payload = res;
            callback(err, res);
            if (err) throw err;
          },
        ]);

        return payload;
      } else {
        const candidate = executor([
          ...args,
          (err, res) => {
            payload = res;
            if (err) throw err;
          },
        ]);
        return candidate === undefined ? payload : candidate;
      }
    }

    const payload = hope();

    if (callback) {
      const candidate = executor([
        ...argsWithoutCb,
        (err, res) => {
          callback(err, res);
          if (err) return payload.reject(err);
          return payload.resolve(res);
        },
      ]);

      if (candidate !== undefined) return candidate;
      return payload;
    } else {
      const candidate = executor([
        ...argsWithoutCb,
        (err, res) => {
          if (err) return payload.reject(err);
          return payload.resolve(res);
        },
      ]);

      if (candidate !== undefined) return candidate;
      return payload;
    }
  }

  handler.bind(self);

  return handler;
}
