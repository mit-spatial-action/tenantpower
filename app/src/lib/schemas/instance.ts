import { z } from 'zod';

export const instanceSchema = z.object({
  title: z.string(),
  description: z.string()
});

// Infer the type from the schema for use in your app
export type Config = z.infer<typeof instanceSchema>;