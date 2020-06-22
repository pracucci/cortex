import React, { FC, useState } from 'react';
import { Link } from '@reach/router';
import { Collapse, Navbar, NavbarToggler, Nav, NavItem, NavLink } from 'reactstrap';
import PathPrefixProps from '../types/PathPrefixProps';

const Navigation: FC<PathPrefixProps> = ({ pathPrefix }) => {
  const [isOpen, setIsOpen] = useState(false);
  const toggle = () => setIsOpen(!isOpen);
  return (
    <Navbar className="mb-3" dark color="dark" expand="md" fixed="top">
      <NavbarToggler onClick={toggle} />
      <Link className="navbar-brand" to={`${pathPrefix}/`}>
        Cortex Deep View
      </Link>
      <Collapse isOpen={isOpen} navbar style={{ justifyContent: 'space-between' }}>
        <Nav className="ml-0" navbar>
          <NavItem>
            <NavLink href={`${pathPrefix}/storage/bucket`}>Bucket</NavLink>
          </NavItem>
        </Nav>
      </Collapse>
    </Navbar>
  );
};

export default Navigation;
