const flightService = require('./flight.service.js')
const logger = require('../../services/logger.service')

module.exports = {
   getFlights,
   getFlightsGroup,
   getFlightsGroupList,
   getFlightById,
   addFlight,
   updateFlight,
   removeFlight
}

async function getFlights(req, res) {
   try {
      logger.debug('Getting Flights')
      var filter = req.query || {}
      // console.log(filter);
      const flights = await flightService.query(filter)
      res.json(flights)
   } catch (err) {
      logger.error('Failed to get flights', err)
      res.status(500).send({ err: 'Failed to get flights' })
   }
}

async function getFlightsGroup(req, res) {
   try {
      logger.debug('Getting Flights Groups')
      const group = req.query || {}
      const flights = await flightService.queryGroup(group)
      res.json(flights)
   } catch (err) {
      logger.error('Failed to get flights groups', err)
      res.status(500).send({ err: 'Failed to get flights groups' })
   }
}

async function getFlightsGroupList(req, res) {
   try {
      logger.debug('Getting Flights Groups List')
      var filter = req.query || {}
      const flights = await flightService.queryGroupList(filter)
      res.json(flights)
   } catch (err) {
      logger.error('Failed to get flights groups List', err)
      res.status(500).send({ err: 'Failed to get flights groups List' })
   }
}

async function getFlightById(req, res) {
   try {
      const flightId = req.params.flightId
      const flight = await flightService.getById(flightId)
      res.json(flight)
   } catch (err) {
      logger.error('Failed to get flight', err)
      res.status(500).send({ err: 'Failed to get flight' })
   }
}

async function addFlight(req, res) {
   try {
      const flight = req.body
      const addedFlight = await flightService.add(flight)
      // res.json(addedFlight)
      res.send(addedFlight)
   } catch (err) {
      logger.error('Failed to add flight', err)
      res.status(500).send({ err: 'Failed to add flight' })
   }
}

async function updateFlight(req, res) {
   try {
      const flight = req.body
      const updatedFlight = await flightService.update(flight)
      res.json(updatedFlight)
   } catch (err) {
      logger.error('Failed to update flight', err)
      res.status(500).send({ err: 'Failed to update flight' })
   }
}

async function removeFlight(req, res) {
   try {
      const flightId = req.params.flightId
      const removedId = await flightService.remove(flightId)
      res.send(removedId)
   } catch (err) {
      logger.error('Failed to remove flight', err)
      res.status(500).send({ err: 'Failed to remove flight' })
   }
}
