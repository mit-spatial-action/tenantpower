<script lang="ts">
	import favicon from '$lib/assets/favicon.svg';
	import '@fontsource-variable/overpass';
	import '@fontsource-variable/overpass-mono';
	import '$lib/styles/global.scss';
	let { children } = $props();
</script>

<svelte:head>
	<link rel="icon" href={favicon} />
</svelte:head>

{@render children()}

<style>
  :global(body) {
    margin: 0;
    padding: 0;
  }
</style>