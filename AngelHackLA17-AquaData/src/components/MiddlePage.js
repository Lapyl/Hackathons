import React, {Component} from 'react'
import {Button} from 'react-materialize'

class MiddlePage extends Component {
  constructor(){
    super()
    this.chooseLeak=this.chooseLeak.bind(this)
  }
  chooseLeak(){
    this.props.chooseLeak()
  }

  render(){

    return (
      <div className='text-align center'>


      <h5>What would you like to report?</h5>

      <div className="col s12 m7" onClick={this.chooseLeak}>

        <div className="card horizontal hoverable">
          <div className="card-image">
            <img src="./icon1.png"/>
          </div>
          <div className="card-stacked">
            <div className="card-content">
              <span className="card-title">Environmental</span>
              <p>
              drought, contamination, storm water run-off, spills</p>
            </div>
            <div className="card-action">
              <a className="right-align teal-text text-lighten-4" href="#">Report</a>
            </div>
          </div>
        </div>
      </div>

      <div className="col s12 m7">

  <div className="card horizontal hoverable">
    <div className="card-image">
      <img  src="./icon2.png"/>
    </div>
    <div className="card-stacked">
      <div className="card-content">
        <span className="card-title">Infrastructure</span>
        <p>aging pipes, leaks, damages</p>
      </div>
      <div className="card-action">
        <a className="right-align teal-text text-lighten-4" href="#">Report</a>
      </div>
    </div>
  </div>
</div>

<div className="col s12 m7">

  <div className="card horizontal hoverable">
    <div className="card-image">
      <img  src="./icon3.png"/>
    </div>
    <div className="card-stacked">
      <div className="card-content">
        <span className="card-title">Health</span>
        <p>unusual symptoms, warnings, outbreaks</p>
      </div>
      <div className="card-action">
        <a className="right-align teal-text text-lighten-4" href="#">Report</a>
      </div>
    </div>
  </div>
</div>

      </div>
    )
  }
}

export default MiddlePage
