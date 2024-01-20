import { prisma } from "./prism";

async function test() {
  const start = Date.now();

  const results = await Promise.all([
    totalActiveBrokers(),
    activeBrokersByCity(),
    activeBrokersByMonth(),
    topActiveBrokersByCity(),
    changeInBrokersFromLastMonthByPercentage(),
    topCityByChangeInBroker(),
  ]);

  console.log('Total Active Brokers:', results[0]);
  console.log('Active Brokers By City:', results[1]);
  console.log('Active Brokers By Month:', results[2]);
  console.log('Top Active Brokers By City:', results[3]);
  console.log('Change In Brokers From Last Month By Percentage:', results[4]);
  console.log('Top City By Change In Broker:', results[5]);

  const end = Date.now();
  console.log(`Execution time: ${end - start} ms`);
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
  return highestDiffCity;
}

export async function topActiveBrokersByCity(): Promise<{
  city: string;
  count: number;
} | undefined> {
  const currentDate = new Date();
  const currentYear = currentDate.getUTCFullYear();
  const currentMonth = currentDate.getUTCMonth() + 1;
  const result = await prisma.broker.groupBy({
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
    take: 1,
  });
  return result.map((item) => ({ city: item.city, count: item._count.city }))[0];
}
//find city with most new brokers this month from last month
export async function changeInBrokersFromLastMonthByPercentage(): Promise<number> {
  const currentDate = new Date();
  const currentYear = currentDate.getUTCFullYear();
  const currentMonth = currentDate.getUTCMonth() + 1;
  const lastMonth = currentMonth === 1 ? 12 : currentMonth - 1;
  const lastYear = currentMonth === 1 ? currentYear - 1 : currentYear;

  const currentMonthCount = await prisma.brokerActivity.count({
    where: {
      AND: [
        { year: currentYear },
        { month: currentMonth },
        { isActive: true },
      ],
    },
  });

  const lastMonthCount = await prisma.brokerActivity.count({
    where: {
      AND: [
        { year: lastYear },
        { month: lastMonth },
        { isActive: true },
      ],
    },
  });

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
  const result = await prisma.broker.groupBy({
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

  const res = await prisma.brokerActivity.groupBy({
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
  const res = await prisma.broker.count({
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
  });
  return res as number;
}