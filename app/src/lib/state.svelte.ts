import type { FeatureCollection } from 'geojson';
import type { Component } from 'svelte';

export const appState = $state({
    isGeocoding: false as boolean,
    errors: {
        geocode: false as boolean
    },
    isInit: true as boolean,
    selected: null as FeatureCollection | null,
    highlighted: null as number | null
});

export const modalState = $state({
  isOpen: false,
  component: null as Component | null,
  
  async open(fileName: string) {
    this.isOpen = true;
    try {
      const module = await import(`./content/${fileName}.md`);
      this.component = module.default;
    } catch (e) {
      console.error("Failed to load modal content", e);
      this.close();
    }
  },

  close() {
    this.isOpen = false;
    this.component = null;
  }
});
