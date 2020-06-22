import React, { FC } from 'react';
import { RouteComponentProps } from '@reach/router';
import PathPrefixProps from '../../types/PathPrefixProps';
import { Link } from '@reach/router';
import { useFetch } from '../../hooks/useFetch';

type GetBucketResponse = {
    tenants?: string[]
};

interface BucketContentProps {
    error?: Error;
    data?: GetBucketResponse;
}

export const BucketContent: FC<BucketContentProps> = ({ error, data }) => {
    // TODO Check if an error occurred

    // Check if it's still loading.
    if (!data) {
        return (
            <>Loading...</>
        )
    }

    // Sort users.
    let users = data.tenants || []
    users.sort()

    return (
        <>
            <h2>Bucket</h2>
            <ul>
                {users.map((name, index) => {
                    return <li key={index}>
                        {/* TODO add pathPrefix */}
                        <Link to={`/storage/bucket/${name}`}>
                            {name}
                        </Link>
                    </li>
                })}
            </ul>
        </>
    );
};

const Bucket: FC<RouteComponentProps & PathPrefixProps> = ({ pathPrefix }) => {
    const { response, error } = useFetch<GetBucketResponse>("http://localhost:3002/api/v1/storage/bucket");
    return <BucketContent error={error} data={response.data} />;
};

export default Bucket;
