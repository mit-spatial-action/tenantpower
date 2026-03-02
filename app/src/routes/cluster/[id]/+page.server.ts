import type { PageServerLoad } from './$types';

export const load: PageServerLoad = async ({ params, fetch }) => {
    const { id } = params;
    const response = await fetch(`/api/clusters/${id}`);

    if (!response.ok) return null;
    return await response.json();
};