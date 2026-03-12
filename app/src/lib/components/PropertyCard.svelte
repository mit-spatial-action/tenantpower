<script lang="ts">
	import { fly } from 'svelte/transition';
	import { USDollar, muniExpand } from '$lib/helpers/helpers';
	import type { Feature } from 'geojson';

	let { feature }: { feature: Feature } = $props();

	const prop = $derived(feature?.properties ?? {});
</script>

{#snippet tag(label: string, value: string|number)}
    {#if value}
        <div class="control">
            <div class="tags has-addons">
                <span class="tag has-text-weight-bold">{label}</span>
                <span class="tag">{value}</span>
            </div>
        </div>
    {/if}
{/snippet}

{#if prop.prop_addr}
	<div transition:fly={{ x: -200, duration: 200 }} class="bordered box mx-2 mt-2">
		<div class="block">
			<div class="block has-background-primary p-3">
				<h2 class="title is-3 has-text-white">{prop.prop_addr}</h2>
				<h3 class="subtitle is-4 has-text-white">{muniExpand(prop.town)}</h3>
			</div>

			<div class="field is-grouped is-grouped-multiline">
                {@render tag("Year", prop.year)}
                {@render tag("Value", USDollar.format(prop.ass_val))}
                {@render tag("Sale Price", USDollar.format(prop.sale_p))}
                {@render tag("Sale Date", new Date(prop.sale_d).toLocaleDateString('en-US'))}
				<span class="tag is-primary has-text-weight-bold">
					<a href="/nearby" class="has-text-white">What's Nearby?</a>
				</span>
			</div>
		</div>

		{#if prop.own}
			<h4 class="title is-4">Owner</h4>
			<div class="box bordered">
				<div class="block has-background-primary p-2">
					<p class="has-text-weight-bold has-text-white">{prop.own}</p>
					{#if prop.own_city}
						<p class="has-text-white">
							{prop.own_city}{#if prop.own_state}, {prop.own_state}{/if}
						</p>
					{/if}
				</div>
				{#if prop.count > 1}
					<div class="field is-grouped is-grouped-multiline">
                        {@render tag("Other Properties", prop.count - 1)}
						<span class="tag is-primary has-text-weight-bold">
							<a href="/cluster/{prop.clst}/{prop.id}" class="has-text-white">What Else Do They Own?</a>
						</span>
					</div>
				{/if}
			</div>
		{/if}
	</div>
{/if}