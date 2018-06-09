import React, {Component} from 'react'
import superagent from 'superagent'
import {Button} from 'react-materialize'

class ReportForm extends Component {
  constructor(){
    super()
    this.submitReport=this.submitReport.bind(this);
  }

  submitReport(){
    this.props.submitReport(this.refs.long.value,this.refs.lat.value);
    if(this.refs.long.value !== '' && this.refs.lat.value !== ''){
      superagent
      .get(`/sendText/${this.refs.lat.value}/${this.refs.long.value}`)
      .query(null)
      .set('Accept','text/json')
      .end((err,response)=>{
        console.log(response.body)
      })
    }
  }

  render(){


    return (
      <div className='text-align center'>
        <Button>Test Report Form</Button>

        <div className="container">
          <div className="section">


            <h5>Where is the leak?</h5>

            <div className="row">
              <form className="col s12">

                <div className="row">

                  <div className="input-field col s6">
                    <input  id="first_name" type="text" className="validate"/>
                    <label htmlFor="first_name">Street Address *</label>
                  </div>

                  <div className="input-field col s6">
                    <input id="last_name" type="text" className="validate"/>
                    <label htmlFor="last_name">City *</label>
                  </div>
                </div>

                <div className="row">
                  <div className="input-field col s6">
                    <input id="lat" ref="lat" type="text" className="validate"/>
                    <label htmlFor="lat">Latitude</label>
                  </div>
                  <div className="input-field col s6">
                    <input id="long" ref="long" type="text" className="validate"/>
                    <label htmlFor="long">Longitude</label>
                  </div>
                </div>

                <h5>How bad is the leak?</h5>
                <h6>Please select from the options below *</h6>



                  <p>
                    <input type="checkbox" id="test5" />
                    <label htmlFor="test5">Drip</label>
                  </p>

                  <p>
                    <input type="checkbox" id="test5" />
                    <label htmlFor="test5">Flowing</label>
                  </p>

                  <p>
                    <input type="checkbox" id="test5" />
                    <label htmlFor="test5">Flowing Down Street</label>
                  </p>

                  <p>
                    <input type="checkbox" id="test5" />
                    <label htmlFor="test5">Burst</label>
                  </p>


                <div className="input-field col s12">
                  <select>
                    <option value="" disabled selected>Type of leak *</option>
                    <option value="1">Meter Leak</option>
                    <option value="2">Street Leak</option>
                    <option value="3">Hydrant Leak</option>
                    <option value="4">Other Leak</option>
                  </select>
                  <label>Materialize Select</label>
                </div>



                  <div className="file-field input-field">
                    <div className="btn">
                      <span>File</span>
                      <input type="file"/>
                    </div>
                    <div className="file-path-wrapper">
                      <input className="file-path validate" type="text"/>
                    </div>
                  </div>



                Your Contact Details

                <div className="row">
                  <div className="input-field col s12">
                    <input id="password" type="password" className="validate"/>
                    <label htmlFor="password">Name</label>
                  </div>
                </div>
                <div className="row">
                  <div className="input-field col s12">
                    <input id="email" type="email" className="validate"/>
                    <label htmlFor="email">Phone Number</label>
                  </div>
                </div>
                <div className="row">
                  <div className="col s12">
                    Email:
                    <div className="input-field inline">
                      <input id="email" type="email" className="validate"/>
                      <label htmlFor="email" data-error="wrong" data-success="right">janedoe@gmail.com</label>
                    </div>
                  </div>
                </div>

              </form>
            </div>

            <a onClick={this.submitReport} className="waves-effect waves-light btn">submit</a>

          </div>
        </div>

        <footer className="page-footer teal">
        </footer>

      </div>
    )
  }
}

export default ReportForm
