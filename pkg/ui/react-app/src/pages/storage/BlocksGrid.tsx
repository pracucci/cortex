import React, { Component, MouseEvent, ChangeEvent } from "react";
import {Block} from "./Tenant"
import "./BlocksGrid.scss"
import {Link} from "@reach/router";
import moment from "moment";
import Slider from 'rc-slider';
import 'rc-slider/assets/index.css';
const { Range } = Slider;

type BlockViewProps = {
    userID: string,
    block: Block
    viewMinTime: number
    viewMaxTime: number
};

type BlockViewState = {
    detailsEnabled: boolean
};

class BlockView extends Component<BlockViewProps, BlockViewState> {
    constructor(props: BlockViewProps) {
        super(props)

        // Initialize state.
        this.state = {
            detailsEnabled: false,
        }
    }

    onMouseEnter(event: MouseEvent) {
        this.setState({
            detailsEnabled: true,
        })
    }

    onMouseLeave(event: MouseEvent) {
        this.setState({
            detailsEnabled: false,
        })
    }

    render() {
        let { block, userID, viewMinTime, viewMaxTime } = this.props
        let { detailsEnabled } = this.state

        // Calculate the span width (in %).
        let viewWidth = viewMaxTime - viewMinTime;
        let spanWidth = ((block.max_time - block.min_time) / viewWidth) * 100;

        // Apply a min width to make sure that the block span is always visible
        // within the view.
        if (spanWidth < 0.25) {
            spanWidth = 0.25
        }

        // Calculate the left offset (in %).
        let spanOffset = ((block.min_time - viewMinTime) / viewWidth) * 100;

        // Generate snippet for details (if enabled).
        let details
        if (detailsEnabled) {
            // TODO add block statistics + uploaded at

            details = (
                <div className="block-details">
                    <div className="detail">
                        <div className="name">Min time</div>
                        <div className="value">{moment.unix(block.min_time / 1000).utc().format()}</div>
                    </div>
                    <div className="detail">
                        <div className="name">Max time</div>
                        <div className="value">{moment.unix(block.max_time / 1000).utc().format()}</div>
                    </div>
                    <div className="detail">
                        <div className="name">Time range</div>
                        <div className="value">{moment.duration(block.max_time - block.min_time, "ms").humanize()}</div>
                    </div>
                    <div className="detail">
                        <div className="name">Compaction level</div>
                        <div className="value">{block.compaction.level}</div>
                    </div>
                </div>
            )
        }

        // TODO click on the span should toggle the opened "stickiness"

        // TODO marked for deletion
        return (
            <div className={`block  compaction-level-${block.compaction.level}`} onMouseEnter={this.onMouseEnter.bind(this)} onMouseLeave={this.onMouseLeave.bind(this)}>
                <div className="block-id">
                    <Link to={`/storage/bucket/${userID}/${block.block_id}`}>{block.block_id}</Link>
                </div>
                <div className="block-info">
                    <div className="block-span">
                        <span style={{width: `${spanWidth}%`, marginLeft: `${spanOffset}%`}}/>
                    </div>

                    { details }
                </div>
            </div>
        )
    }
};

type BlocksGridProps = {
    tenant_id: string
    blocks: Block[]
};

type BlocksGridState = {
    gridMinTime: number
    gridMaxTime: number
    viewMinTime: number
    viewMaxTime: number
    filter: string
};

class BlocksGrid extends Component<BlocksGridProps, BlocksGridState> {
    constructor(props: BlocksGridProps) {
        // Sort blocks by min time.
        props.blocks.sort((first, second) => {
            return first.min_time - second.min_time
        })

        super(props)

        // Find the min and max time across all blocks of the view.
        let gridMinTime = props.blocks[0].min_time
        let gridMaxTime = props.blocks[0].max_time
        props.blocks.forEach((block) => {
            if (block.min_time < gridMinTime) {
                gridMinTime = block.min_time
            }
            if (block.max_time > gridMaxTime) {
                gridMaxTime = block.max_time
            }
        })

        // Initialize the state.
        this.state = {
            gridMinTime: gridMinTime,
            gridMaxTime: gridMaxTime,
            viewMinTime: gridMinTime,
            viewMaxTime: gridMaxTime,
            filter: "",
        }
    }

    onRangeChange(values: Array<number>) {
        this.setState({
            viewMinTime: values[0],
            viewMaxTime: values[1],
        })
    }

    onSearchChange(event: ChangeEvent<HTMLInputElement>) {
        this.setState({
            filter: event.currentTarget.value.toUpperCase(),
        })
    }

    render() {
        let { blocks, tenant_id } = this.props
        let { viewMinTime, viewMaxTime, gridMinTime, gridMaxTime, filter } = this.state

        // Filter blocks by view's timerange and search.
        blocks = blocks.filter((block: Block) => {
            return block.max_time > viewMinTime &&
                block.min_time < viewMaxTime &&
                (filter === "" || block.block_id.includes(filter))
        })

        // TODO fix UI: it breaks when there are no blocks after filtering
        // TODO filter block id(s)

        return (
            <>
                <input type="text" placeholder="Search blocks" onChange={this.onSearchChange.bind(this)}/>

                <div className="blocks-grid">
                    <div className="time-picker">
                        <div></div>
                        <div>
                            <Range
                                allowCross={false}
                                min={gridMinTime}
                                max={gridMaxTime}
                                defaultValue={[gridMinTime, gridMaxTime]}
                                onChange={this.onRangeChange.bind(this)} />
                        </div>
                    </div>

                    <div className="time-range">
                        <div></div>
                        <div>
                            <div className="time-label min">
                                {moment.unix(viewMinTime / 1000).utc().format()}
                            </div>
                            <div className="time-label max">
                                {moment.unix(viewMaxTime / 1000).utc().format()}
                            </div>
                        </div>
                    </div>

                    {blocks.map((block) => {
                        return <BlockView
                            key={block.block_id}
                            block={block}
                            viewMinTime={viewMinTime}
                            viewMaxTime={viewMaxTime}
                            userID={tenant_id}
                        />
                    })}
                </div>
            </>
        )
    }
}

export default BlocksGrid;
