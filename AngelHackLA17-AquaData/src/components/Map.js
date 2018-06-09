import React, { Component } from 'react'
import { GoogleMapLoader, GoogleMap, Marker } from 'react-google-maps'
import superagent from 'superagent'

class Map extends Component {


  componentDidMount(){
    const url = '/getIncidents'
    superagent
    .get(url)
    .query(null)
    .set('Accept','text/json')
    .end((err,response)=>{
      console.log(response);
      const venues = response.body
      console.log(JSON.stringify(venues))
      this.props.setMarkers(venues)
  })
}
  render(){
    const mapContainer = <div style={{height: '100%', width:'100%'}}></div>

    const markers = this.props.markers.map((venue,i)=>{
      const marker = {
        position: {
          lat:parseFloat(venue.lat),
          lng:parseFloat(venue.lng)
        }
      }
    return <Marker key = {i} {...marker}/>
  })


    return (
      <GoogleMapLoader
        containerElement = {mapContainer}
        googleMapElement = {
          <GoogleMap
          defaultZoom={13}
          defaultCenter={this.props.center}
          options={{streetViewControl:false, mapTypeControl:false}}>
          {markers}
          </GoogleMap>
        } />






    )
  }
}

export default Map
