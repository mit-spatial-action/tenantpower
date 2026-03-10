<script lang="ts">
  import favicon from "$lib/assets/favicon.png";
  import favicon32 from "$lib/assets/favicon_32x32.png";
  import favicon192 from "$lib/assets/favicon_192x192.png";
  import SvelteSeo from "svelte-seo";
  import "@fontsource-variable/overpass";
  import "iconify-icon";
  import { page, navigating } from '$app/state';
  import Overlay from "$lib/components/Overlay.svelte";
  import Spinner from "$lib/components/Spinner.svelte";
  import Map from "$lib/components/Map.svelte";
  import type { Config } from "$lib/schemas/instance";

  import { config } from "$lib/config";
  const { title, description, canonical, keywords, images } = config;

  const assetFiles = import.meta.glob('$lib/assets/*.{png,jpg,jpeg,svg}', { 
    eager: true, 
    import: 'default' 
  });

  const resolvedImages = images.map(img => ({
    ...img,
    url: assetFiles[`/src/lib/assets/${img.url}`] 
  })) as Config["images"];

  let kw=keywords.join(",")

  import "$lib/styles/global.scss";
  let { children } = $props();
</script>

<svelte:head>
  <link rel="icon" href={favicon} type="image/png" sizes="16x16" />
  <link rel="icon" href={favicon32} type="image/png" sizes="32x32" />
  <link rel="icon" href={favicon192} type="image/png" sizes="192x192" />
  <link rel="apple-touch-icon" href={favicon192} sizes="192x192" />
</svelte:head>


{#if navigating.to}
  <Spinner/>
{/if}

<SvelteSeo
  {title}
  {description}
  {canonical}
  keywords={kw}
  openGraph={{
    title: title,
    description: description,
    images: resolvedImages,
    url: canonical,
    type: "website",
    site_name: title
  }}
/>

<Map/>
<Overlay>
  {@render children()}
</Overlay>

<style>
  :global(body) {
    margin: 0;
    padding: 0;
  }
</style>
