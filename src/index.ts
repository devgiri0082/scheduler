import dotenv from "dotenv";
import axios from "axios";
dotenv.config();
import fs from 'fs/promises';

import cron from 'node-cron';
import { AxiosResponseType, Record, Records, fetchDataType } from "./types";
import { prisma } from "./prism";


// Schedule the main function to run every second
cron.schedule('0 0 1 * *', main);

async function main() {
  try {
    const allRecord: Records = await getAllRecordsForCurrentMonth();
    const fileName = `records-${new Date().getUTCFullYear()}-${new Date().getUTCMonth() + 1}.json`;
    await saveRecordsToFile(fileName, allRecord);
    const allRecordObjs: { [key: string]: Record } = {};
    allRecord.forEach((record) => {
      if (record["מס רשיון"]) {
        allRecordObjs[record["מס רשיון"]] = record;
      } else {
        console.log('record without license number', record);
      }
    });
    await createBrokerActivityForCurrentMonth(allRecordObjs);
    await createNewBrokersAndActivities(Object.values(allRecordObjs));
  } catch (e: any) {
    console.log(e);
  }
}




async function saveRecordsToFile(fileName: string, allRecord: Records) {
  await fs.writeFile(`./${fileName}`, JSON.stringify({ records: allRecord }));
  console.log('successfully saved the records to file');
}
async function getAllRecordsForCurrentMonth(): Promise<Records> {
  let offset = 0;
  let limit = 5000;
  let total = Infinity;
  const allRecord: Records = [];

  while (offset < total) {
    if (process.env.API_URL === undefined) throw new Error("API_URL is not defined");
    const res = await fetchData({ api: process.env.API_URL, offset: offset, limit: limit });
    const data = AxiosResponseType.parse(res.data.result);
    // console.log(data, data.records.length, offset, limit, total);
    allRecord.push(...data.records);
    data.fields
    offset += limit;
    total = data.total;
  }
  console.log('fetching records successful records count: ', allRecord.length)
  return allRecord;
}

async function createBrokerActivityForCurrentMonth(allRecordObj: { [key: string]: Record }) {
  try {
    const currentDate = new Date();
    const currentYear = currentDate.getUTCFullYear();
    const currentMonth = currentDate.getUTCMonth() + 1;

    const brokers = await prisma.broker.findMany();
    const brokerActivities = brokers.map((broker) => {
      let isActive = false;
      if (allRecordObj[broker.licenseNumber]) {
        isActive = delete allRecordObj[broker.licenseNumber];
      }
      return {
        year: currentYear,
        month: currentMonth,
        isActive: isActive,
        brokerId: broker.id,
      };
    });

    const brokerActivity = await prisma.brokerActivity.createMany({
      data: brokerActivities,
      skipDuplicates: true, // Skip duplicates to avoid conflicts
    });

    console.log(`BrokerActivity creation for the current month completed successfully. count ${brokerActivity.count}`);
  } catch (error) {
    console.error('Error creating BrokerActivity:', error);
  }
}


async function createNewBrokersAndActivities(records: Record[]) {
  const newBrokers = await prisma.broker.createMany({
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

  const brokerActivity = await prisma.brokerActivity.createMany({
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


function fetchData({ offset, limit, api }: fetchDataType): Promise<any> {
  return axios.post(api, {
    resource_id: "a0f56034-88db-4132-8803-854bcdb01ca1",
    q: "",
    filters: {
      not_count_request: "0"
    },
    limit,
    offset
  })

}
