import "vitest";

declare module "vitest" {
  export interface ProvidedContext {
    redisUrl: string;
  }
}
