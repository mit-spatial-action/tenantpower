<script lang="ts">
    let { active = $bindable(false), children } = $props();
    import { fade } from "svelte/transition";
    const close = () => (active = false);
</script>

{#if active}
    <div class="modal is-active" transition:fade={{ duration: 200 }}>
        <div class="modal-background"></div>
        {@render children?.(close)}
    </div>
{/if}
