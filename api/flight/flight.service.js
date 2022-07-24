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

async function query(filter) {
   try {
      const criteria = _buildCriteria(filter)
      // const criteriaSort = _buildCriteriaSort(filterBy)

      const collection = await dbService.getCollection('flight')

      const lastRefresh = await collection
         .find()
         .sort({ createdAt: -1 })
         .limit(1)
         .project({ 'createdAt': 1, '_id': 0 })
         .toArray()

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

      if (Object.keys(criteria).length) {
         criteria.CHAORD = filter.board
         criteria.CHFLTN = { $regex: "^[0-9]*$" }

         const flights = await collection
            .find(criteria)
            .sort({ 'CHSTOL': -1 })
            .toArray()
         return { flights, lastRefresh, minDate, maxDate }
      } else {
         const flights = await collection
            .find({ 'CHAORD': filter.board, 'CHFLTN' : { $regex: "^[0-9]*$" } })
            .sort({ 'CHSTOL': -1 })
            .limit(50)
            .toArray()
         return { flights, lastRefresh, minDate, maxDate }
      }

   } catch (err) {
      logger.error('cannot find flights', err)
      throw err
   }
}

async function queryGroupList(filter) {
   try {
      const collection = await dbService.getCollection('flight')
      const flights = await collection.find({ 'CHAORD': filter.board, 'CHFLTN' : { $regex: "^[0-9]*$" } }).project({ 'CHOPERD': 1, 'CHFLTN': 1, 'CHRMINE': 1, 'CHLOCCT': 1, 'CHSTOL': 1, '_id': 0 }).sort({}).toArray()
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
      if (group.field === 'CHRMINE') match = { 'CHAORD': group.board, 'CHRMINE': 'CANCELED', 'CHFLTN' : { $regex: "^[0-9]*$" } }
      else match = { 'CHAORD': group.board, 'CHRMINE': statusFilght, 'CHFLTN' : { $regex: "^[0-9]*$" } }

      let field = ''
      if (group.field === 'CHRMINE') field = '$CHSTOL'
      else field = `$${group.field}`

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
            $group: {
               // _id: `$${group['0']}`,
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
   const criteria = {}

   if (flightNo) {
      criteria.CHFLTN = flightNo
      // criteria.CHFLTN = { $regex: flightNo, $options: 'i' }
   }

   if (flightCompany) {
      criteria.CHOPERD = flightCompany
      // criteria.CHOPERD = { $regex: flightCompany, $options: 'i' }
   }

   if (destination) {
      criteria.CHLOCCT = destination
      // criteria.CHLOCCT = { $regex: destination, $options: 'i' }
   }

   if (status) {
      // criteria.CHRMINE = status 
      criteria.CHRMINE = { $regex: status, $options: 'i' }
   }

   if (scheduleDate) {
      criteria.CHSTOL = scheduleDate
      criteria.CHSTOL = { $regex: scheduleDate, $options: 'i' }
      criteria.CHSTOL = {
         "$gte": `${scheduleDate}T00:00:00.000Z`,
         "$lt": `${scheduleDate}T23:59:59.000Z`
      }
      // criteria = { $expr: {$eq: [scheduleDate, { $dateToString: {date: "$CHSTOL", format: "%Y-%m-%d"}}]}}
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

