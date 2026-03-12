import adapter from '@sveltejs/adapter-netlify';
import { mdsvex } from 'mdsvex';

/** @type {import('@sveltejs/kit').Config} */
const config = {
	extensions: ['.svelte', '.md', '.svx'],
	preprocess: [
		mdsvex({ extensions: ['.md', '.svx'] })
	],
	kit: {
		adapter: adapter({
			edge: false,
			split: false
		})
	}
};

export default config;
