import { prisma } from "./prisma";
import { DataSet, Record } from "./types";

async function test() {
  const dataset1: DataSet = {
    currentMonthData: [
      {
        "_id": 1,
        "מס רשיון": 3161589,
        "שם המתווך": "קרויטורו-באטה שרית שרה",
        "עיר מגורים": "כפר יונה"
      },
      {
        "_id": 2,
        "מס רשיון": 30412376,
        "שם המתווך": "לסט צפורה",
        "עיר מגורים": "רחובות"
      },
      {
        "_id": 3,
        "מס רשיון": 3182424,
        "שם המתווך": "ילין קרני",
        "עיר מגורים": "תל אביב - יפו"
      },
      {
        "_id": 4,
        "מס רשיון": 311106002,
        "שם המתווך": "כהן חיים",
        "עיר מגורים": "רמת גן"
      },
      {
        "_id": 5,
        "מס רשיון": 3182423,
        "שם המתווך": "קנטור גוז צביה",
        "עיר מגורים": "תל אביב - יפו"
      },
      {
        "_id": 6,
        "מס רשיון": 3161447,
        "שם המתווך": "יהושפט שירלי",
        "עיר מגורים": "גבעת עדה"
      },
      {
        "_id": 7,
        "מס רשיון": 31035664,
        "שם המתווך": "עקילוב ירון",
        "עיר מגורים": "תל אביב"
      },
      {
        "_id": 8,
        "מס רשיון": 3172678,
        "שם המתווך": "גורניצקי אבי",
        "עיר מגורים": "תל אביב - יפו"
      },
      {
        "_id": 9,
        "מס רשיון": 3133933,
        "שם המתווך": "וגנר אילת",
        "עיר מגורים": "נתניה"
      },
      {
        "_id": 10,
        "מס רשיון": 30929023,
        "שם המתווך": "בן דוד דוד",
        "עיר מגורים": "רמת השרון"
      }
    ],
    currentMonth: 1,
    currentYear: 2024,
    nextMonth: 12,
    nextYear: 2023,
    nextMonthData: [
      {
        "_id": 1,
        "מס רשיון": 3161589,
        "שם המתווך": "קרויטורו-באטה שרית שרה",
        "עיר מגורים": "כפר יונה"
      },
      {
        "_id": 2,
        "מס רשיון": 30412376,
        "שם המתווך": "לסט צפורה",
        "עיר מגורים": "רחובות"
      },
      {
        "_id": 3,
        "מס רשיון": 3182424,
        "שם המתווך": "ילין קרני",
        "עיר מגורים": "תל אביב - יפו"
      },
      {
        "_id": 4,
        "מס רשיון": 311106002,
        "שם המתווך": "כהן חיים",
        "עיר מגורים": "רמת גן"
      },
      {
        "_id": 5,
        "מס רשיון": 3182423,
        "שם המתווך": "קנטור גוז צביה",
        "עיר מגורים": "תל אביב - יפו"
      },
      {
        "_id": 6,
        "מס רשיון": 3161447,
        "שם המתווך": "יהושפט שירלי",
        "עיר מגורים": "גבעת עדה"
      },
      {
        "_id": 7,
        "מס רשיון": 31035664,
        "שם המתווך": "עקילוב ירון",
        "עיר מגורים": "תל אביב"
      },
      {
        "_id": 8,
        "מס רשיון": 3172678,
        "שם המתווך": "גורניצקי אבי",
        "עיר מגורים": "תל אביב - יפו"
      },
      {
        "_id": 9,
        "מס רשיון": 3133933,
        "שם המתווך": "וגנר אילת",
        "עיר מגורים": "נתניה"
      },
      {
        "_id": 10,
        "מס רשיון": 30929023,
        "שם המתווך": "בן דוד דוד",
        "עיר מגורים": "רמת השרון"
      },
      {
        "_id": 11,
        "מס רשיון": 3133940,
        "שם המתווך": "וגנר אילת",
        "עיר מגורים": "נתניה"
      },
      {
        "_id": 12,
        "מס רשיון": 30929050,
        "שם המתווך": "בן דוד דוד",
        "עיר מגורים": "רמת השרון"
      }
    ]
  };

  await clearTestDB();
  console.log('clearing test db done');
  await addToTestDB(dataset1.currentMonthData, { month: dataset1.currentMonth, year: dataset1.currentYear });
  await addToTestDB(dataset1.nextMonthData, { month: dataset1.nextMonth, year: dataset1.nextYear });
  console.log("successfully added to the db");
  //add count
  const totalBrokers = await prisma.brokerTest.findMany({ select: { id: true } });
  const totalBrokerActivity = await prisma.brokerActivityTest.findMany({ select: { id: true } });
  console.log('total broker in db should be 12 is : ', totalBrokers.length);
  console.log('total activity in the db shoul db 22 is : ', totalBrokerActivity.length);

  const results = await Promise.all([
    totalActiveBrokers(),
    topActiveBrokersByCity(),
    activeBrokersByMonth(),
    changeInBrokersFromLastMonthByPercentage(),
    topCityByChangeInBroker(),
  ]);

  console.log('Total Active Brokers this month:', results[0]);
  console.log('Active Brokers By City this month:', results[1]);
  console.log('Active Brokers By Month:', results[2]);
  console.log('Top Active Brokers By City:', results[1][0]);
  console.log('Change In Brokers From Last Month By Percentage:', results[3]);
  console.log('Top City By Change In Broker:', results[4]);
};

test();



async function clearTestDB() {
  await prisma.brokerActivityTest.deleteMany({});
  await prisma.brokerTest.deleteMany({});
}



async function addToTestDB(dataset: Record[], { month, year }: { month: number, year: number }) {
  const recordObj: { [key: string]: Record } = {};
  dataset.forEach((record) => {
    if (record["מס רשיון"]) {
      recordObj[record["מס רשיון"]] = record;
    } else {
      console.log('record without license number', record);
    }
  });

  await createBrokerActivityTestForGivenMonth(recordObj, { month, year });
  await createNewBrokersTestAndActivitiesTestForGivenMonth(Object.values(recordObj), { month, year });

}


async function createBrokerActivityTestForGivenMonth(allRecordObj: { [key: string]: Record }, { month, year }: { month: number, year: number }) {
  try {

    const brokers = await prisma.brokerTest.findMany();
    const brokerActivities = brokers.map((broker) => {
      let isActive = false;
      if (allRecordObj[broker.licenseNumber]) {
        isActive = delete allRecordObj[broker.licenseNumber];
      }
      return {
        year: year,
        month: month,
        isActive: isActive,
        brokerId: broker.id,
      };
    });

    const brokerActivity = await prisma.brokerActivityTest.createMany({
      data: brokerActivities,
      skipDuplicates: true, // Skip duplicates to avoid conflicts
    });

    console.log(`BrokerActivity creation for the current month completed successfully. count ${brokerActivity.count}`);
  } catch (error) {
    console.error('Error creating BrokerActivity:', error);
  }
}

async function createNewBrokersTestAndActivitiesTestForGivenMonth(records: Record[], { month, year }: { month: number, year: number }) {
  const newBrokers = await prisma.brokerTest.createMany({
    data: records.map(record => ({
      id: record._id,
      licenseNumber: record["מס רשיון"]!,
      name: record["שם המתווך"] || '',
      city: record["עיר מגורים"] || '',
      createdAt: new Date(),
      lastUpdate: new Date(),
    })),
  });
  console.log(`New Broker creation for the current month completed successfully. count ${newBrokers.count}`)

  const brokerActivity = await prisma.brokerActivityTest.createMany({
    data: records.map((broker, index) => ({
      year: new Date().getUTCFullYear(),
      month: new Date().getUTCMonth() + 1,
      isActive: true,
      brokerId: broker._id
    })),
    skipDuplicates: true, // Skip duplicates to avoid conflicts
  });

  console.log(`New Broker with activity creation for the current month completed successfully. count ${brokerActivity.count}`);
}


export async function topCityByChangeInBroker() {
  const currentDate = new Date();
  const currentYear = currentDate.getUTCFullYear();
  const currentMonth = currentDate.getUTCMonth() + 1;
  const lastMonth = currentMonth === 1 ? 12 : currentMonth - 1;
  const lastYear = currentMonth === 1 ? currentYear - 1 : currentYear;

  const currentMonthActiveBrokers = await activeBrokersByCityByMonth({ month: currentMonth, year: currentYear });
  const lastMonthActiveBrokers = await activeBrokersByCityByMonth({ month: lastMonth, year: lastYear });
  const currentObj: { [key: string]: number } = {};
  currentMonthActiveBrokers.forEach((item) => currentObj[item.city] = item.count);
  const lastObj: { [key: string]: number } = {};
  lastMonthActiveBrokers.forEach((item) => lastObj[item.city] = item.count);
  //find highest difference city between current and last month
  let highestDiffCity = '';
  let highestDiff = 0;
  Object.keys(currentObj).forEach((city) => {
    const diff = currentObj[city]! - (lastObj[city] ?? 0);
    if (diff > highestDiff) {
      highestDiff = diff;
      highestDiffCity = city;
    }
  })
  return { city: highestDiffCity, count: highestDiff };
}

export async function topActiveBrokersByCity(): Promise<{
  city: string;
  count: number;
}[]> {
  const currentDate = new Date();
  const currentYear = currentDate.getUTCFullYear();
  const currentMonth = currentDate.getUTCMonth() + 1;
  const result = await prisma.brokerTest.groupBy({
    by: ['city'],
    where: {
      activities: {
        some: {
          AND: [
            { year: currentYear },
            { month: currentMonth },
            { isActive: true },
          ],
        },
      },
    },
    _count: {
      city: true,
    },
    orderBy: {
      _count: {
        city: 'desc',
      },
    },
  });
  return result.map((item) => ({ city: item.city, count: item._count.city }));
}
//find city with most new brokers this month from last month
export async function changeInBrokersFromLastMonthByPercentage(): Promise<number> {
  const currentDate = new Date();
  const currentYear = currentDate.getUTCFullYear();
  const currentMonth = currentDate.getUTCMonth() + 1;
  const lastMonth = currentMonth === 1 ? 12 : currentMonth - 1;
  const lastYear = currentMonth === 1 ? currentYear - 1 : currentYear;

  const currentMonthCount = await totalActiveBrokersByMonth({ month: currentMonth, year: currentYear });
  const lastMonthCount = await totalActiveBrokersByMonth({ month: lastMonth, year: lastYear });

  console.log({ currentMonth, currentYear, lastMonth, lastYear, currentMonthCount, lastMonthCount })

  return (currentMonthCount - lastMonthCount) / (lastMonthCount > 0 ? lastMonthCount : currentMonthCount) * 100;

}

export async function activeBrokersByCity() {
  const currentDate = new Date();
  const currentYear = currentDate.getUTCFullYear();
  const currentMonth = currentDate.getUTCMonth() + 1;
  const result = await activeBrokersByCityByMonth({ month: currentMonth, year: currentYear });
  return result;
}

async function activeBrokersByCityByMonth({ month, year }: { month: number, year: number }): Promise<{
  city: string;
  count: number;
}[]> {
  const currentDate = new Date();
  const currentYear = currentDate.getUTCFullYear();
  const currentMonth = currentDate.getUTCMonth() + 1;
  const result = await prisma.brokerTest.groupBy({
    by: ['city'],
    where: {
      activities: {
        some: {
          AND: [
            { year: year },
            { month: month },
            { isActive: true },
          ],
        },
      },
    },
    _count: true,
  });
  return result.map((item) => ({ city: item.city, count: item._count }));
}

export async function activeBrokersByMonth(): Promise<{
  month: string | undefined;
  count: number;
}[]> {
  const currentDate = new Date();
  const currentYear = currentDate.getUTCFullYear();
  const currentMonth = currentDate.getUTCMonth() + 1;

  const res = await prisma.brokerActivityTest.groupBy({
    by: ['month'],
    where: {
      AND: [
        { year: currentYear },
        { month: { gte: 0, lte: 11 } },
        { isActive: true },
      ],
    },
    _count: true,
    orderBy: {
      month: 'asc',
    },
  });
  const monthLetterToName: { [key: number]: string } = {
    0: 'Jan',
    1: 'Feb',
    2: 'Mar',
    3: 'Apr',
    4: 'May',
    5: 'Jun',
    6: 'Jul',
    7: 'Aug',
    8: 'Sep',
    9: 'Oct',
    10: 'Nov',
    11: 'Dec'
  }

  return res.map((item) => ({ month: monthLetterToName[item.month - 1], count: item._count }));
}

export async function totalActiveBrokers(): Promise<number> {
  const currentDate = new Date();
  const currentYear = currentDate.getUTCFullYear();
  const currentMonth = currentDate.getUTCMonth() + 1;
  const total = await totalActiveBrokersByMonth({ month: currentMonth, year: currentYear });
  return total;
}

export async function totalActiveBrokersByMonth({ month, year }: { month: number, year: number }): Promise<number> {

  const res = await prisma.brokerTest.count({
    where: {
      activities: {
        some: {
          AND: [
            { year: year },
            { month: month },
            { isActive: true },
          ],
        },
      },
    },
  });
  return res as number;
}