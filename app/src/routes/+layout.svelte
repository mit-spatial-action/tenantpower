<script lang="ts">
  import favicon from "$lib/assets/favicon.ico";
  import favicon32 from "$lib/assets/favicon_32x32.png";
  import favicon192 from "$lib/assets/favicon_192x192.png";
  // import SvelteSeo from "svelte-seo";
  import "@fontsource-variable/overpass";
  import "@fontsource-variable/overpass-mono";

  import _config from "$lib/config.json";
  import { ConfigSchema, type Config } from "$lib/config";
  const result = ConfigSchema.safeParse(_config);
  const config: Config = result.data!;

  import "$lib/styles/global.scss";
  let { children } = $props();
</script>

<svelte:head>
  <link rel="icon" href={favicon} type="image/x-icon" sizes="16x16" />
  <link rel="icon" href={favicon32} type="image/png" sizes="32x32" />
  <link rel="icon" href={favicon192} type="image/png" sizes="192x192" />
  <link rel="apple-touch-icon" href={favicon192} sizes="192x192" />
</svelte:head>

{@render children()}

<style>
  :global(body) {
    margin: 0;
    padding: 0;
  }
</style>
