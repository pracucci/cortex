import React, {FC} from "react";
import {Block} from "./Tenant"
import {Link, RouteComponentProps} from "@reach/router";
import PathPrefixProps from "../../types/PathPrefixProps";
import {useFetch} from "../../hooks/useFetch";
import moment from "moment";

type GetBlockResponse = {
    tenant_id: string,
    block_id: string,
    block: Block,
};

type BlockViewContentProps = {
    error?: Error;
    data?: GetBlockResponse;
};

const BlockViewContent = ({ error, data }: BlockViewContentProps) => {
    // TODO Check if an error occurred

    // Check if it's still loading.
    if (!data) {
        return (
            <>Loading...</>
        )
    }

    let block = data.block;
    let blockTimeRange = block.max_time - block.min_time;

    // TODO size of index, index-header and chunks
    // TODO block statistics
    // TODO compaction details (parents)

    return (
        <>
            <h2><Link to={`/storage/bucket`}>Bucket</Link> / <Link to={`/storage/bucket/${data.tenant_id}`}>{data.tenant_id}</Link> / {block.block_id}</h2>
            <table className="table">
                <tbody>
                <tr>
                    <th scope="row">Min time</th>
                    <td>{moment.unix(block.min_time / 1000).utc().format()}</td>
                    <td>{block.min_time} ms</td>
                </tr>
                <tr>
                    <th scope="row">Max time</th>
                    <td>{moment.unix(block.max_time / 1000).utc().format()}</td>
                    <td>{block.max_time} ms</td>
                </tr>
                <tr>
                    <th scope="row">Time range</th>
                    <td>{moment.duration(blockTimeRange, "ms").humanize()}</td>
                    <td>{blockTimeRange} ms</td>
                </tr>
                <tr>
                    <th scope="row">Compaction level</th>
                    <td>{block.compaction.level}</td>
                    <td></td>
                </tr>
                </tbody>
            </table>
        </>
    )
};


type BlockViewRouteParams = {
    tenant_id: string,
    block_id: string,
};


const BlockView: FC<RouteComponentProps<BlockViewRouteParams> & PathPrefixProps> = ({ pathPrefix, tenant_id, block_id }) => {
    // TODO pathPrefix
    const { response, error } = useFetch<GetBlockResponse>(`http://localhost:3002/api/v1/storage/bucket/${tenant_id}/${block_id}`);
    return <BlockViewContent error={error} data={response.data} />;
};

export default BlockView;
