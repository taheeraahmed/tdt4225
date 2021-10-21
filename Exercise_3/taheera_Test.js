// # Find all types of transportation modes and count how many distinct users that
// # have used the different transportation modes. Do not count the rows where the
// # transportation mode is null


/****************** THIS IS SOLVABLE IN PYTHON *********************/

db.Activity.aggregate([
  { $unwind: "$transportation_mode" },
  {
      $group: {
          _id: {$toLower: '$transportation_mode'},
          //count: { $sum: 1 },
          user_ids: { $push: '$$ROOT._user_id' },
      }
  },
  { $sort : { count : -1} },
]);


/***********************************************************************/

db.Activity.aggregate([
  { $unwind: '$_user_id' },
  {
    $group: { _id: { 
      trans_name: '$transportation_mode' }, 
      count: { $sum: 1 } },
      user_ids: { $push: '$$ROOT._user_id' },
  },
  {
    $group: {
      _id: { 
        user_id: '$_id.user_id', 
        trans_name: '$_id.trans_name' },
      totalCount: { $sum: '$count', $sum: 'user_ids' },
      distinctCount: { $sum: 1 },
    },
  },
  { $sort: { count: -1 } },
]);

/***********************************************************************/

db.Activity.aggregate([
  {
    $group: { _id: { trans_name: '$transportation_mode' }, count: { $sum: 1 } },
  },
  {
    $group: {
      _id: { user_id: '$_id.user_id', trans_name: '$_id.trans_name' },
      totalCount: { $sum: '$count' },
      distinctCount: { $sum: 1 },
    },
  },
  { $sort: { count: -1 } },
]);

/***********************************************************************/

db.Activity.aggregate([
  { $unwind: '$transportation_mode' },
  {
    $group: {
      _id: {
        trans_name: '$transportation_mode',
      },
      user_id: { $push: '$$ROOT._user_id' },
      count: { $sum: 1 },
    },
  },
  {
    $group: {
      _id: {
        user_id: '$_id.user_id',
        trans_name: '$_id.trans_name',
      },

      totalCount: { $sum: '$user_id' },
      distinctCount: { $sum: 1 },
    },
  },

  { $sort: { count: -1 } },
]);

/***********************************************************************/

db.Activity.aggregate([
  { $unwind: '$_user_id' },
  {
    $group: {
      _id: null,
      transportation_mode: { $push: '$$ROOT.transportation_mode' },
      user_id: { $push: '$$ROOT._user_id' },
    },
  },
]);

db.Activity.aggregate([
  { $unwind: '$transportation_mode' },
  {
    $group: {
      _id: {},
    },
  },
]);

db.Activity.aggregate([
  { $unwind: '$transportation_mode' },
  {
    $group: {
      _id: null,
      count: { $sum: 1 },
    },
  },
  { $sort: { count: -1 } },
]);

db.Activity.aggregate([
  { $unwind: '$transportation_mode' },
  { $group: { _id: '$_user_id' } },
]);
