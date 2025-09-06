"use client";

import { faker } from "@faker-js/faker";

export function generateRandomName(groupId: string): string {
  const random_seed = Math.floor(parseInt(groupId, 36));
  console.debug("Generating name with seed:", random_seed);
  faker.seed(random_seed);

  return faker.person.fullName();
}
