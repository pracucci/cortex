import React, { FC } from 'react';
import { Container } from 'reactstrap';
import { Router } from '@reach/router';

import { Bucket, Tenant, BlockView } from './pages/storage';
import PathPrefixProps from './types/PathPrefixProps';
import Navigation from './components/Navbar';
import { ErrorBoundary } from './components';

const App: FC<PathPrefixProps> = ({ pathPrefix }) => {
  return (
    <ErrorBoundary>
      <Navigation pathPrefix={pathPrefix} />
      <Container fluid style={{ paddingTop: 70 }}>
        <Router basepath={`${pathPrefix}`}>

          {/*
            NOTE: Any route added here needs to also be added to the list of
            React-handled router paths ("reactRouterPaths") in /web/web.go.
          */}
          <Bucket path="/storage/bucket" pathPrefix={pathPrefix} />
          <Tenant path="/storage/bucket/:tenant_id" pathPrefix={pathPrefix} />
          <BlockView path="/storage/bucket/:tenant_id/:block_id" pathPrefix={pathPrefix} />
        </Router>
      </Container>
    </ErrorBoundary>
  );
};

export default App;
