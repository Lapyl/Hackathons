import React, {Component} from 'react'
import Map from './Map'
import ReportForm from './ReportForm'
import superagent from 'superagent'
import {Button, Icon,Row, Input} from 'react-materialize'
import Icons from './Icons'
import MiddlePage from './MiddlePage.js'

class Main extends Component {
  constructor(){
    super()
    this.state = {
      venues:[],
      page:'welcome'
    }
    this.handleClick=this.handleClick.bind(this);
    this.handleButton=this.handleButton.bind(this);
    this.submitReport=this.submitReport.bind(this);
    this.closeIcons=this.closeIcons.bind(this);
    this.handleMarkers=this.handleMarkers.bind(this);
    this.handleLeak=this.handleLeak.bind(this);
  }

  handleClick(){
    // $('.button-collapse').sideNav('hide')
    this.setState({
      page:'IconPage'
    })
  }
  handleLeak(){
    // $('.button-collapse').sideNav('hide')
    this.setState({
      page:'LeakReport'
    })
  }

  submitReport(long,lat){
    console.log(this.state)
    this.setState({
      page:'welcome',
      venues: [
        ...this.state.venues,
        {
          type:"leak",
          lng:parseFloat(long),
          lat:parseFloat(lat)
        }
      ]
    })
  }
  closeIcons(){
    this.setState({
      page:'welcome'
    })
  }

  handleButton(){
    this.setState({
      page:'ReportForm'
    })
  }
  handleMarkers(venues){
    this.setState({
      venues: [
        ...this.state.venues,
        ...venues
      ]
    })

  }

  componentDidMount(){
    console.log('componentDidMount')

    const url = '/getIncidents'
    superagent
    .get(url)
    .query(null)
    .set('Accept','text/json')
    .end((err,response)=>{
      console.log(response);
      const venues = response.body
      console.log(JSON.stringify(venues))
      this.setState({
        venues:venues
      })
    })
  }


  render(){
    const location ={
      lat:40.7575286,
      lng:-73.988
    }
    if(this.state.page === 'welcome'){
      return (
        <div className='text-align center'>
          <ul id="slide-out" className="side-nav">
            <li><div className="userView">
              <img src="./logo.jpg" alt="Google User" ></img>

              <a href="#!name"><span className="name">Jon Doe</span></a>
              <a href="#!email"><span className="email">jdandturk@gmail.com</span></a>
            </div></li>
            <li><a href="#!"><i className="material-icons" onClick={this.handleClick}>card_giftcard</i>See Your Awards</a></li>
            <li><a href="#!"><i className="material-icons" onClick={this.handleClick}>info</i>Information</a></li>
            <li><div className="divider"></div></li>
            <li><a href="#!"><i className="material-icons" onClick={this.handleClick}>settings</i>Settings</a></li>
          </ul>
          <a href="#" data-activates="slide-out" className="button-collapse"><i className="material-icons">menu</i></a>
          <Button node='a' waves='light'>Welcome</Button>


          <div style={{width:600,height:400}}>
            <Map center={location} markers={this.state.venues} setMarkers={this.handleMarkers}/>
          </div>

          <Row>
      <Input placeholder="Search Location" s={12} label="Search" />
    </Row>
    <Button waves='light' onClick={this.handleButton}>Report<i className="material-icons">location_on</i></Button>


        </div>
      )
    } else if(this.state.page==="IconPage"){
      return (
        <Icons closeIcons={this.closeIcons}/>
      )
    } else if(this.state.page==='ReportForm'){
      return (
        <div>
        <MiddlePage chooseLeak={this.handleLeak}/>
      </div>
      )
    } else if(this.state.page==='LeakReport'){
      return (
        <div>
        <ReportForm submitReport={this.submitReport}/>
      </div>
      )
    }


  }
}

export default Main
