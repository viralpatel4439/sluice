import type { RedisLike, SubscriberConn } from "./adapters.js";

export type Handler = (channel: string, message: string) => void;

/**
 * One subscriber connection shared by many listeners, with reference counts
 * per channel. Re-subscribes everything after a reconnect.
 */
export class SubscriptionHub {
  private readonly channels = new Map<string, Set<Handler>>();
  private refs = 1;

  constructor(
    private readonly sub: SubscriberConn,
    private readonly owned: boolean,
  ) {
    sub.onMessage((channel, message) => {
      const set = this.channels.get(channel);
      if (set) for (const h of set) h(channel, message);
    });
    sub.onReconnect(() => {
      for (const ch of this.channels.keys()) void sub.subscribe(ch).catch(() => {});
    });
  }

  /** Open a dedicated subscriber connection for `client`. */
  static async create(client: RedisLike): Promise<SubscriptionHub> {
    return new SubscriptionHub(await client.subscriber(), true);
  }

  retain(): this {
    this.refs++;
    return this;
  }

  async subscribe(channel: string, handler: Handler): Promise<void> {
    let set = this.channels.get(channel);
    if (!set) {
      set = new Set();
      this.channels.set(channel, set);
      set.add(handler);
      await this.sub.subscribe(channel);
      return;
    }
    set.add(handler);
  }

  async unsubscribe(channel: string, handler: Handler): Promise<void> {
    const set = this.channels.get(channel);
    if (!set) return;
    set.delete(handler);
    if (set.size === 0) {
      this.channels.delete(channel);
      await this.sub.unsubscribe(channel).catch(() => {});
    }
  }

  release(): void {
    if (--this.refs <= 0 && this.owned) this.sub.close();
  }
}
