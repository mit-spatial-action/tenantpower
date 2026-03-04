import rawConfig from './instance.json';
import { instanceSchema } from '$lib/schemas/instance';

export const config = instanceSchema.parse(rawConfig);