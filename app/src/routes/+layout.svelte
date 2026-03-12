<script lang="ts">
  import favicon from "$lib/assets/favicon.png";
  import favicon32 from "$lib/assets/favicon_32x32.png";
  import favicon192 from "$lib/assets/favicon_192x192.png";
  import SvelteSeo from "svelte-seo";
  import "@fontsource-variable/overpass";
  import Overlay from "$lib/components/Overlay.svelte";
  import Spinner from "$lib/components/Spinner.svelte";
  import Map from "$lib/components/Map.svelte";
  import type { Component } from 'svelte';
  import type { Config } from "$lib/schemas/instance";
  import Modal from "$lib/components/Modal.svelte";
  import Geocoder from "$lib/components/Geocoder.svelte";
  import { navigating } from '$app/state';
  import { appState } from '$lib/state.svelte';
  import { modalState } from '$lib/state.svelte';
  let { children } = $props();

  import "$lib/styles/global.scss";
  import { config } from "$lib/config";
  const { title, description, canonical, keywords, images } = config;

  const assetFiles = import.meta.glob("$lib/assets/*.{png,jpg,jpeg,svg}", {
    eager: true,
    import: "default",
  });

  const resolvedImages = images.map((img) => ({
    ...img,
    url: assetFiles[`/src/lib/assets/${img.url}`],
  })) as Config["images"];

  let isBusy = $derived(navigating.to !== null || appState.isGeocoding);

  modalState.open('warning')
</script>

<svelte:head>
  <link rel="icon" href={favicon} type="image/png" sizes="16x16" />
  <link rel="icon" href={favicon32} type="image/png" sizes="32x32" />
  <link rel="icon" href={favicon192} type="image/png" sizes="192x192" />
  <link rel="apple-touch-icon" href={favicon192} sizes="192x192" />
</svelte:head>

<SvelteSeo
  {title}
  {description}
  {canonical}
  keywords={keywords.join(",")}
  openGraph={{
    title: title,
    description: description,
    images: resolvedImages,
    url: canonical,
    type: "website",
    site_name: title,
  }}
/>

<Map />

<Modal 
  spinning={isBusy} 
  isOpen={modalState.isOpen} 
  close={() => modalState.close()}
>
  {#if modalState.component}
    <modalState.component />
  {/if}
</Modal>

<Overlay>
  <div class="bordered mx-2 mt-2">
    <nav class="navbar" aria-label="main navigation">
      <div class="navbar-brand">
        <a class="navbar-item mt-2" href={canonical}>
          <h1 class="title">{title}</h1>
        </a>
      </div>
      <div class="navbar-end">
        <div class="navbar-item">
          <div class="buttons">
            <button
              class="button is-primary is-normal is-responsive"
              onclick={() => modalState.open('about')}>About</button
            >
          </div>
        </div>
      </div>
    </nav>
    <Geocoder />
  </div>
  {@render children()}
</Overlay>

<style lang="scss">
  :global(body) {
    margin: 0;
    padding: 0;
  }

  :global(.bordered, .mapboxgl-ctrl-geocoder) {
    border-bottom: 0.25rem solid red;
    border-right: 0.25rem solid red;
  }
</style>
