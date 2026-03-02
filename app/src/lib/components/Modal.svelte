<script lang="ts">
    import { fade, fly } from "svelte/transition";
    let { title = null, children } = $props();
    let active = $state(true);
    const onclick = () => (active = !active);
</script>

{#if active}
    <div class="modal is-active" transition:fade={{ duration: 200 }}>
        <div class="modal-background"></div>
        <div class="modal-card" transition:fly={{ y: 600, duration: 800 }}>
            {#if title}
                <header class="modal-card-head has-background-primary">
                    <h1 class="modal-card-title title is-3 has-text-white">
                        {title}
                    </h1>
                </header>
            {/if}
            <section class="modal-card-body">
                {@render children?.()}
            </section>
            <footer class="modal-card-foot">
                <button class="button is-primary is-fullwidth" {onclick}
                    >Continue→</button
                >
            </footer>
        </div>
    </div>
{/if}
