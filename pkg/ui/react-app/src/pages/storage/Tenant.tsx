import React, {FC} from "react";
import {Link, RouteComponentProps} from "@reach/router";
import PathPrefixProps from "../../types/PathPrefixProps";
import {useFetch} from "../../hooks/useFetch";
import BlocksGrid from "./BlocksGrid";

export type Block = {
    block_id: string,
    min_time: number,
    max_time: number,
    compaction: BlockCompaction,
};

export type BlockCompaction = {
    level: number,
};

type GetTenantResponse = {
    tenant_id: string,
    blocks: Block[],
};

interface TenantContentProps {
    error?: Error;
    data?: GetTenantResponse;
}

const TenantContent: FC<TenantContentProps> = ({ error, data }) => {
    // TODO Check if an error occurred

    // Check if it's still loading.
    if (!data) {
        return (
            <>Loading...</>
        )
    }

    // TODO rename tenant to user
    // TODO handle the case there are no blocks (do not render the BlocksGrid at all)

    return (
        <>
            <h2><Link to={`/storage/bucket`}>Bucket</Link> / {data.tenant_id}</h2>
            <BlocksGrid blocks={data.blocks} tenant_id={data.tenant_id} />
        </>
    );
};

type TenantRouteParams = {
    tenant_id: string,
};

const Tenant: FC<RouteComponentProps<TenantRouteParams> & PathPrefixProps> = ({ pathPrefix, tenant_id }) => {
    // TODO pathPrefix
    const { response, error } = useFetch<GetTenantResponse>(`http://localhost:3002/api/v1/storage/bucket/${tenant_id}`);
    return <TenantContent error={error} data={response.data} />;
};

export default Tenant;
