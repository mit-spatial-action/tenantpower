<script lang="ts">
    import { fade, fly } from "svelte/transition";
    import { Square } from "svelte-loading-spinners";
    import type { Snippet } from "svelte";
    let {
        isOpen = false,
        close = () => {},
        children,
        spinning = false,
    } = $props<{
        isOpen?: boolean;
        close?: () => void;
        children?: Snippet;
        spinning?: boolean;
    }>();
</script>

{#if isOpen || spinning}
    <div class="modal is-active" transition:fade={{ duration: 200 }}>
        <div class="modal-background"></div>
        {#if spinning}
            <div transition:fly={{ y: 300, duration: 300 }}>
                <Square size="60" color="#FF0000" unit="px" duration="1s" />
            </div>
        {:else if children}
            <div
                class="modal-card bordered"
                transition:fly={{ y: 600, duration: 800 }}
            >
                <section class="modal-card-body">
                    <div class="content">
                        {@render children?.(close)}
                    </div>
                </section>
                <footer class="modal-card-foot">
                    <button
                        class="button is-primary is-fullwidth"
                        onclick={close}>Continue →</button
                    >
                </footer>
            </div>
        {/if}
    </div>
{/if}
