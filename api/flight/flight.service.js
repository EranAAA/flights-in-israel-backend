const dbService = require('../../services/db.service')
const logger = require('../../services/logger.service')
const ObjectId = require('mongodb').ObjectId

module.exports = {
   remove,
   query,
   queryGroup,
   queryGroupList,
   getById,
   add,
   update
}

async function getDates() {
   try {
      const collection = await dbService.getCollection('flight')
      const minDate = await collection
         .find()
         .sort({ CHSTOL: -1 })
         .limit(1)
         .project({ 'CHSTOL': 1, '_id': 0 })
         .toArray()
      const maxDate = await collection
         .find()
         .sort({ CHSTOL: 1 })
         .limit(1)
         .project({ 'CHSTOL': 1, '_id': 0 })
         .toArray()
      const lastRefresh = await collection
         .find()
         .sort({ createdAt: -1 })
         .limit(1)
         .project({ 'createdAt': 1, '_id': 0 })
         .toArray()

      return { minDate, maxDate, lastRefresh }
   } catch (err) {
      logger.error('cannot find dates', err)
      throw err
   }
}

async function query(filter) {
   const { lastRefresh, minDate, maxDate } = await getDates()

   try {
      const criteria = _buildCriteria(filter)
      const collection = await dbService.getCollection('flight')

      if (criteria.$and.length) {
         criteria.$and.push({ 'CHFLTN': { $regex: "^[0-9]*$" } })
         criteria.$and.push({ 'CHAORD': filter.board })
         const flights = await collection
            .find(criteria)
            .sort({ 'CHSTOL': -1 })
            .toArray()
         return { flights, lastRefresh, minDate, maxDate }

      } else {
         const flights = await collection
            .find({ 'CHAORD': filter.board, 'CHFLTN': { $regex: "^[0-9]*$" } })
            .sort({ 'CHSTOL': -1 })
            .limit(50)
            .toArray()
         return { flights, lastRefresh, minDate, maxDate }
      }

   } catch (err) {
      logger.error('cannot find flights', err)
      const flights = [{
         _id: "",
         CHOPER: "",
         CHFLTN: "",
         CHOPERD: '',
         CHSTOL: '',
         CHPTOL: "",
         CHAORD: "",
         CHLOC1D: "",
         CHLOC1T: "Didn't found any result",
         CHLOCCT: "",
         CHTERM: "",
         CHRMINE: "",
         createdAt: 1659020408309
      }]
      return { flights, lastRefresh, minDate, maxDate }
      // throw err
   }
}

async function queryGroupList(filter) {
   try {
      const collection = await dbService.getCollection('flight')
      const flights = await collection.aggregate(
         [
            {
               $match: { 'CHAORD': filter.board, 'CHFLTN': { $regex: "^[0-9]*$" } }
            },
            { $group: { "_id": { CHAORD: "$CHAORD", CHOPERD: "$CHOPERD", CHLOCCT: "$CHLOCCT" } } }
         ]
      ).toArray()
      return flights

   } catch (err) {
      logger.error('cannot find flights group List', err)
      throw err
   }
}

async function queryGroup(group) {

   try {
      const collection = await dbService.getCollection('flight')

      let statusFilght = ''
      if (group.board === 'D') statusFilght = 'DEPARTED'
      else if (group.board === 'A') statusFilght = 'LANDED'

      let match = ''
      if (group.field === 'CHRMINE') match = { /*'CHAORD': group.board,*/ 'CHRMINE': 'CANCELED', 'CHFLTN': { $regex: "^[0-9]*$" } }
      else match = { /*'CHAORD': group.board,*/ 'CHRMINE': { $in: ["DEPARTED", "LANDED"] }, 'CHFLTN': { $regex: "^[0-9]*$" } }

      let field = ''
      if (group.field === 'CHRMINE') field = { _id: '$SCHEDULED', CHAORD: '$CHAORD' }
      else if (group.field === 'DATE') field = { _id: '$SCHEDULED', CHAORD: '$CHAORD' }
      else if (group.field === 'HOUR') field = { _id: '$SCHEDULED_HOUR', CHAORD: '$CHAORD' }
      else field = { _id: `$${group.field}`, CHAORD: '$CHAORD' } // `$${group.field}`

      const flights = await collection.aggregate([
         {
            $match: match
         },
         {
            $addFields: {
               CHSTOL: { $dateFromString: { dateString: "$CHSTOL" } },
               CHPTOL: { $dateFromString: { dateString: "$CHPTOL" } }
            }
         },
         {
            $addFields: {
               difference: {
                  $divide: [{ $subtract: ["$CHPTOL", "$CHSTOL",] }, 60000]
               }
            }
         },
         {
            $addFields: {
               SCHEDULED: { $substr: ["$CHSTOL", 0, 10] },
               SCHEDULED_HOUR: { $hour: "$CHSTOL" },
            }
         },
         {
            $group: {
               _id: field,
               totalDifference: { $sum: "$difference" },
               countFlights: { $count: {} }
            }
         },
      ]).sort({ 'totalDifference': -1 }).toArray()

      return flights

   } catch (err) {
      logger.error('cannot find flights group', err)
      throw err
   }
}

async function getById(flightId) {
   try {
      const collection = await dbService.getCollection('flight')
      const flight = collection.findOne({ _id: ObjectId(flightId) })
      return flight
   } catch (err) {
      logger.error(`while finding flight ${flightId}`, err)
      throw err
   }
}

async function remove(flightId) {
   try {
      const collection = await dbService.getCollection('flight')
      await collection.deleteOne({ _id: ObjectId(flightId) })
      return flightId
   } catch (err) {
      logger.error(`cannot remove flight ${flightId}`, err)
      throw err
   }
}

async function add(flight) {
   try {
      const collection = await dbService.getCollection('flight')
      const addedFlight = await collection.insertOne(flight)
      return addedFlight.ops[0]
   } catch (err) {
      logger.error('cannot insert flight', err)
      throw err
   }
}

async function update(flight) {
   try {
      var id = ObjectId(flight._id)
      delete flight._id
      const collection = await dbService.getCollection('flight')
      await collection.updateOne({ _id: id }, { $set: { ...flight } })
      return flight
   } catch (err) {
      logger.error(`cannot update flight ${flightId}`, err)
      throw err
   }
}

function _buildCriteria({ flightNo, flightCompany, destination, status, scheduleDate }) {
   const criteria = { $and: [] }

   if (flightNo) {
      criteria.$and.push({ CHFLTN: flightNo })
   }

   if (flightCompany) {
      criteria.$and.push({ CHOPERD: flightCompany })
   }

   if (destination) {
      criteria.$and.push({ CHLOCCT: destination })
   }

   if (status) {
      criteria.$and.push({ CHRMINE: status })
   }

   if (scheduleDate) {
      criteria.$and.push({ CHSTOL: { "$gte": `${scheduleDate}T00:00:00.000Z` } })
      criteria.$and.push({ CHSTOL: { "$lte": `${scheduleDate}T23:59:59.000Z` } })
   }

   return criteria
}

function _buildCriteriaSort(filterBy) {
   const criteria = {}

   if (filterBy.sort === 'Higher') {
      criteria.price = -1
   }

   if (filterBy.sort === 'Lower') {
      criteria.price = 1
   }

   if (filterBy.sort === 'Newest') {
      criteria.createdAt = -1
   }

   if (filterBy.sort === 'Oldest') {
      criteria.createdAt = 1
   }

   if (!Object.keys(criteria).length) {
      criteria.createdAt = -1
   }

   return criteria
}

