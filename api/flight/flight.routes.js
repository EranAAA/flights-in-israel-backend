const express = require('express')
const { getFlights, getFlightsGroup, getFlightsGroupList, getFlightById, addFlight, updateFlight, removeFlight } = require('./flight.controller')
const router = express.Router()

router.get('/', getFlights)
router.get('/group', getFlightsGroup)
router.get('/groupList', getFlightsGroupList)
// router.get('/:flightId', getFlightById)
router.post('/', addFlight)
router.put('/:flightId', updateFlight)
router.delete('/:flightId', removeFlight)

module.exports = router
