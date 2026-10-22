import { GenericContainer, type StartedTestContainer } from "testcontainers";

let container: StartedTestContainer | undefined;

/** Starts Redis once for the whole run (or uses SLUICE_TEST_REDIS_URL). */
export async function setup({ provide }: { provide: (key: string, value: string) => void }) {
  let url = process.env.SLUICE_TEST_REDIS_URL;
  if (!url) {
    container = await new GenericContainer(process.env.SLUICE_TEST_REDIS_IMAGE ?? "redis:7-alpine")
      .withExposedPorts(6379)
      .start();
    url = `redis://${container.getHost()}:${container.getMappedPort(6379)}`;
  }
  provide("redisUrl", url);
}

export async function teardown() {
  await container?.stop();
}
