import React, { Component } from 'react'
import {LineChart, XAxis, YAxis, Tooltip, Line, Legend, ReferenceLine} from "recharts"
import Brush from 'recharts/lib/cartesian/Brush'
import {Table, Grid, Dropdown, Segment, Dimmer, Loader} from "semantic-ui-react"
import round from "lodash/round"
import minBy from "lodash/minBy"
import maxBy from "lodash/maxBy"

import './App.css'
import cfConfig from "./cloudFunctionConfig.json"

const cfBaseURL = cfConfig['base_url']
const stockOptions = ["AAPL", "SPY", "XOM", "GOOG", "DIS", "NFLX"].map(e => ({key: e, value: e, text: e}))

function stockTSToChartData(stockTS) {
  
    stockTS = stockTS.sort((a,b) => a.date > b.date ? 1 : -1)

    const labels = stockTS.map(ts => new Date(ts.date))
    const adj_close = stockTS.map(ts => ts.adj_close)

    return {
        labels,
        datasets: [{
            label: "My First dataset",
            fillColor: "rgba(220,220,220,0.2)",
            strokeColor: "rgba(220,220,220,1)",
            pointColor: "rgba(220,220,220,1)",
            pointStrokeColor: "#fff",
            pointHighlightFill: "#fff",
            pointHighlightStroke: "rgba(220,220,220,1)",
            data: adj_close      
        }]
    }
}


function avgDaysBetweenEvents(events) {
    events = events.sort((a,b) => a.date > b.date ? 1 : -1)

    const results = []
    for(let i=0; i < (events.length - 1); i++){
        if(events[i].event === "death_cross"){
            let d1 = new Date(events[i].date)
            let d2 = new Date(events[(i+1)].date)
            let dayDiff = (d2 - d1)/1000/60/60/24
            results.push(dayDiff)
            // console.log(dayDiff)   
        }
    }
    if(results.length > 0) {
        return results.reduce((total, num) => total + num)/results.length

    }
}

function movingAverage(series, days=200) {
    const ma = []
    let rollingSum = 0
    for(let i = 0; i < series.length; i++){
        if(i < days){
            rollingSum += series[i]
            ma.push(null)
        } else {
            rollingSum += (series[i] - series[(i - days)])
            ma.push(rollingSum/days)  
        }
    } 
    return ma
}

function getReturnsNDaysForward(series, events){
    const dateHashMap = {}
    series.forEach((s,idx) => {
        dateHashMap[s.date] = idx
    })

    events.forEach(e => {
        const eventIdx = dateHashMap[e.date]
        const eventAdjClose = series[eventIdx].adj_close
        const return1D = (series[eventIdx + 1].adj_close)/eventAdjClose - 1.0
        const return5D = (series[eventIdx + 5].adj_close)/eventAdjClose - 1.0
        const return10D = (series[eventIdx + 10].adj_close)/eventAdjClose - 1.0
        const return20D = (series[eventIdx + 20].adj_close)/eventAdjClose - 1.0

        const min20DayReturn = (minBy(series.slice(eventIdx, eventIdx + 20), o => o.adj_close).adj_close / eventAdjClose) - 1.0
        const max20DayReturn = (maxBy(series.slice(eventIdx, eventIdx + 20), o => o.adj_close).adj_close / eventAdjClose) - 1.0

        e.return1D = return1D
        e.return5D = return5D
        e.return10D = return10D
        e.return20D = return20D
        e.min20DayReturn = min20DayReturn
        e.max20DayReturn = max20DayReturn
    })
    return events
}


class App extends Component {

    constructor(props) {
        super(props)
        this.state = {
            chartData: [],
            events: [],
            loading: false
        }

        this.onSearchChange = this.onSearchChange.bind(this)
    }



    componentDidMount() {
        this.updateSearch('SPY')
    }

    async getEvents(symbol) {
        return fetch(`${cfBaseURL}/events?symbol=${symbol}`)
            .then(r => r.json())
            .then(j => this.setState({events: j}))
    }

    async getStockHistory(symbol){
        return fetch(`${cfBaseURL}/stocks_history?symbol=${symbol}`)
            .then(r => r.json())
            .then(j => {
                j = j.sort((a,b) => a.date > b.date ? 1 : -1)
                j.forEach(e => {e.norm_vol_20 = e.std_20 / e.sma_20}) //add norm to data
                this.setState({chartData: j})
            })
    }

    async updateSearch(symbol){
        // No Error handling yet...
        this.setState({loading: true})
        await Promise.all([this.getEvents(symbol), this.getStockHistory(symbol)])
        this.setState({loading: false})
    }

    onSearchChange(e, d) {
        this.updateSearch(d.value)
    }

    render() {
        console.warn('state', this.state.loading)
        return (
            <div className="App" style={{marginTop: "20px"}}>
                <Dimmer active={this.state.loading}>
                    <Loader size='massive'>Loading...</Loader>
                </Dimmer>
                <Grid divided='vertically' >
                    <Grid.Row columns={1}>
                        <Grid.Column>
                            <div style={{width: "80%", margin: "auto"}}>
                                <Dropdown
                                    placeholder='Search...'
                                    defaultValue="SPY"
                                    fluid
                                    selection
                                    options={stockOptions}
                                    onChange={this.onSearchChange}
                                />
                                {/* <Input placeholder="Search..." icon='search' fluid/> */}
                            </div>
                        </Grid.Column>
                    </Grid.Row>
                    <Grid.Row columns={2}>
                        <Grid.Column>
                            <div style={{marginTop: "50px"}}/>
                            <h4>Close Price</h4>
                            <LineChart width={730} height={250} data={this.state.chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                                <Brush />
                                <XAxis dataKey="date" />
                                <YAxis label={{value: "Close Price", angle: -90, position: 'insideLeft'}}/>
                                <Tooltip />
                                <Legend />
                                <Line type="monotone" dataKey="adj_close" stroke="#8884d8" dot={false}/>
                                <Line type="monotone" dataKey="sma_20" stroke="red" dot={false}/>
                                <Line type="monotone" dataKey="sma_50" stroke="orange" dot={false}/>
                                <Line type="monotone" dataKey="sma_200" stroke="blue" dot={false}/>

                                {this.state.events.map(e => (
                                    <ReferenceLine key={e.date} x={e.date} stroke={(e.event === "golden_cross") ? "green" : "red"} />
                                ))}
                            </LineChart>
                            <hr style={{marginTop: "50px", marginBottom: "50px"}}/>
                            <h4>Daily Volatility Price</h4>
                            <LineChart width={730} height={250} data={this.state.chartData} margin={{ top: 5, right: 30, left: 20, bottom: 5 }}>
                                <XAxis dataKey="date"  label={{value: "Close", offset: 0, position: "insideBottom"}}/>
                                <YAxis label={{value: "20 Day Vol(normalized)", angle: -90, position: 'insideLeft'}}/>
                                <Line type="monotone" dataKey="norm_vol_20" stroke="#8884d8" dot={false}/>
                            </LineChart>

                        </Grid.Column>
                        <Grid.Column>
                            <div style={{display: "flex", flexDirection:"row"}}>
                                <Segment color="blue" floated="left" style={{margin: "0px", marginRight: "20px"}}> There have been <b>{this.state.events.length}</b> such events since 2010</Segment>
                                <Segment color="red" floated="right" style={{margin: "0px"}}>
                                    <span>Avg days from </span><b>Death Cross</b> to <b>Golden Cross: </b>  <b>{round(avgDaysBetweenEvents(this.state.events), 2)}</b>
                                </Segment>
                            </div>
                            <div style={{overflowX: "auto", marginTop:"20px"}}>
                                <Table celled>
                                    <Table.Header>
                                        <Table.Row>
                                            <Table.HeaderCell style={{width: "500px"}}>Date</Table.HeaderCell>
                                            <Table.HeaderCell>Symbol</Table.HeaderCell>
                                            <Table.HeaderCell>Event</Table.HeaderCell>
                                            <Table.HeaderCell>Return 1 Day</Table.HeaderCell>
                                            <Table.HeaderCell>Return 5 Day</Table.HeaderCell>
                                            <Table.HeaderCell>Return 10 Days</Table.HeaderCell>
                                            <Table.HeaderCell>Return 20 Days</Table.HeaderCell>
                                            <Table.HeaderCell>Min 20 Days</Table.HeaderCell>
                                            <Table.HeaderCell>Max 20 Days</Table.HeaderCell>
                                            <Table.HeaderCell>Volatility 20 Days</Table.HeaderCell>
                                            <Table.HeaderCell>Volatility 50 Days</Table.HeaderCell>
                                        </Table.Row>
                                    </Table.Header>

                                    <Table.Body>
                                        {this.state.events.map((e,idx) => (
                                            <Table.Row key={idx}>
                                                <Table.Cell>{e.date}</Table.Cell>
                                                <Table.Cell>{e.symbol}</Table.Cell>
                                                <Table.Cell>{e.event}</Table.Cell>
                                                {['return_lead_1', 'return_lead_5', 'return_lead_10', 
                                                    'return_lead_20', 'return_min_lead_20', 
                                                    'return_max_lead_20'].map((k, idx) => (
                                                    <Table.Cell key={idx} negative={e[k] < 1.0} positive={e[k] > 1.0}>
                                                            {`${round((e[k] - 1.0) * 100, 3)}%`}
                                                        </Table.Cell>
                                                ))}
                                                <Table.Cell>{round(e.norm_vol_lead_20, 2)}</Table.Cell>
                                                <Table.Cell>{round(e.norm_vol_lead_50, 2)}</Table.Cell>
                                            </Table.Row>
                                        ))}
                                    </Table.Body>
                                </Table>              
                            </div>
                        </Grid.Column>
                    </Grid.Row>
                </Grid>
            </div>
        )
    }
}

export default App
