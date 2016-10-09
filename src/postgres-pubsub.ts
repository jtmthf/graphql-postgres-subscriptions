import {PubSubEngine} from 'graphql-subscriptions/dist/pubsub';
import {Client as PostgresClient, Config as PostgresConfig, DoneCallback, Pool as PostgresPool} from 'pg';


export interface PubSubPgOptions {
  connection: PostgresPool;
  triggerTransform?: (trigger: string) => string;
  connectionListener?: (err: Error | PostgresClient) => void;
}

interface SubsRefsMap {
  [trigger: string]: {
    ids: Array<number>;
    client: PostgresClient;
    done: DoneCallback;
  }
}

export class PostgresPubSub implements PubSubEngine {

  constructor(options: PubSubPgOptions) {
    this.triggerTransform = options.triggerTransform || (trigger => trigger as string);
    this.pgPool = options.connection;

    if (options.connectionListener) {
      this.pgPool.on('connect', options.connectionListener);
      this.pgPool.on('acquire', options.connectionListener);
      this.pgPool.on('error', options.connectionListener);
    }

    this.subscriptionMap = {};
    this.subsRefsMap = {};
    this.currentSubscriptionId = 0;
  }

  public publish(trigger: string, payload: any): boolean {
    // TODO PR graphql-subscriptions to use promises as return value
    this.pgPool.query('NOTIFY $1::text, $2::text', [this.triggerTransform(trigger), JSON.stringify(payload)])
    return true;
  }

  public subscribe(trigger: string, onMessage: Function): Promise<number> {
    const triggerName: string = this.triggerTransform(trigger);
    const id = this.currentSubscriptionId++;
    this.subscriptionMap[id] = [triggerName, onMessage];

    let refs = this.subsRefsMap[triggerName];
    if (refs && refs.ids && refs.ids.length > 0) {
      const newRefs = [...refs.ids, id];
      this.subsRefsMap[triggerName].ids = newRefs;
      return Promise.resolve(id);

    } else {
      return new Promise<number>((resolve, reject) => {
        // TODO Support for pattern subs
        this.pgPool.connect((err, client, done) => {
          if (err) {
            return reject(err);
          }
          client.query('LISTEN $1::text', [triggerName], (err, result) => {
            if (err) {
              return reject(err);
            }
            this.subsRefsMap[triggerName] = {
              ids: [id],
              client: client,
              done
            }
            client.on('notification', this.onMessage.bind(this, triggerName));
            resolve(id);
          });
        });
      });
    }
  }

  public unsubscribe(subId: number) {
    const [triggerName = null] = this.subscriptionMap[subId] || [];
    const refs = this.subsRefsMap[triggerName];

    if (!refs || !refs.ids)
      throw new Error(`There is no subscription of id "${subId}"`);

    let newRefs;
    if (refs.ids.length === 1) {
      refs.done();
      newRefs = [];

    } else {
      const index = refs.ids.indexOf(subId);
      if (index != -1) {
        newRefs = [...refs.ids.slice(0, index), ...refs.ids.slice(index + 1)];
      }
    }

    this.subsRefsMap[triggerName] = newRefs;
  }

  private onMessage(channel: string, message: string) {
    const subscribers = this.subsRefsMap[channel];

    // Don't work for nothing..
    if (!subscribers || !subscribers.ids || !subscribers.ids.length)
      return;

    let parsedMessage;
    try {
      parsedMessage = JSON.parse(message);
    } catch (e) {
      parsedMessage = message;
    }

    subscribers.ids.forEach(subId => {
      // TODO Support pattern based subscriptions
      const [triggerName, listener] = this.subscriptionMap[subId];
      listener(parsedMessage);
    });
  }

  private triggerTransform: (trigger: Trigger) => string;
  private pgPool: PostgresPool;

  private subscriptionMap: {[subId: number]: [string , Function]};
  private subsRefsMap: SubsRefsMap;
  private currentSubscriptionId: number;
}

type Path = Array<string | number>;
type Trigger = string | Path;
