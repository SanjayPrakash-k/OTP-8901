/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 * @NModuleScope SameAccount
 */
/*************************************************************************************
 ***********
 *
 *
 * ${OTP-8901} : ${Monthly Over Due Reminder for Customer}
 *
 *
 **************************************************************************************
 ********
 *
 * Author: Jobin and Jismi IT Services
 *
 * Date Created : 05-June-2025
 *
 * Description : This script is for sending email reminders to Customer about the 
 * pending overdue invoices till previous month. The email notification should contain 
 * the Customer Name, Customer Email, Invoice Document Number, Invoice Amount, Days 
 * Overdue which is attached as a CSV file to the email. The sender of the email should 
 * be Sales rep, if sales rep is not available for the customer then the email should 
 * send by NetSuite Admin.
 *
 *
 * REVISION HISTORY
 *
 * @version 1.0  :  05-June-2025:  The initial build was created by JJ0404
 *
 *
 *
 *************************************************************************************
 **********/
define(['N/email', 'N/log', 'N/record', 'N/search', 'N/file'],
    /**
 * @param{email} email
 * @param{log} log
 * @param{record} record
 * @param{search} search
 * @param{file} file
 */
    (email, log, record, search, file) => {
        /**
         * Defines the function that is executed at the beginning of the map/reduce process and generates the input data.
         * @param {Object} inputContext
         * @param {boolean} inputContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Object} inputContext.ObjectRef - Object that references the input data
         * @typedef {Object} ObjectRef
         * @property {string|number} ObjectRef.id - Internal ID of the record instance that contains the input data
         * @property {string} ObjectRef.type - Type of the record instance that contains the input data
         * @returns {Array|Object|Search|ObjectRef|File|Query} The input data to use in the map/reduce process
         * @since 2015.2
         */

        const getInputData = (inputContext) => {
            let searchResult = searchOverdueDetails();
            log.debug('Search Result', searchResult);
            return searchResult;
        }

        /**
        * Function to search and fetch overdue invoice details of the customers
        * @param {void}
        * @returns {search object}
        */
        function searchOverdueDetails(){
            try{
                return search.create({
                    title: 'Monthly Overdue Invoices JJ',
                    id: 'customsearch_jj_monthly_overdue_invoices',
                    type: 'invoice',

                    filters:
                    [
                        ["type","anyof","CustInvc"], 
                        "AND", 
                        ["status","anyof","CustInvc:A"], 
                        "AND", 
                        ["mainline","is","T"], 
                        "AND", 
                        ["daysoverdue","greaterthan","0"], 
                        "AND", 
                        ["trandate","within","5/16/2025","5/17/2025"], 
                        "AND",
                        // ["trandate","within","lastmonth"],
                        // "AND", 
                        ["customermain.isinactive","is","F"]
                    ],

                    columns:
                    [
                        search.createColumn({name: "tranid", label: "Document Number"}),
                        search.createColumn({name: "entity", label: "Name"}),
                        search.createColumn({name: "email", label: "Email"}),
                        search.createColumn({name: "amount", label: "Amount"}),
                        search.createColumn({name: "daysoverdue", label: "Days Overdue"}),
                        search.createColumn({
                            name: "salesrep",
                            join: "customerMain",
                            label: "Sales Rep"
                        })
                    ]
                });
            } catch(error) {
                log.error('Unexpected error occured while searching overdue details', error.toString());
            }
        }

        /**
         * Defines the function that is executed when the map entry point is triggered. This entry point is triggered automatically
         * when the associated getInputData stage is complete. This function is applied to each key-value pair in the provided
         * context.
         * @param {Object} mapContext - Data collection containing the key-value pairs to process in the map stage. This parameter
         *     is provided automatically based on the results of the getInputData stage.
         * @param {Iterator} mapContext.errors - Serialized errors that were thrown during previous attempts to execute the map
         *     function on the current key-value pair
         * @param {number} mapContext.executionNo - Number of times the map function has been executed on the current key-value
         *     pair
         * @param {boolean} mapContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} mapContext.key - Key to be processed during the map stage
         * @param {string} mapContext.value - Value to be processed during the map stage
         * @since 2015.2
         */

        const map = (mapContext) => {
            processInvoiceData(mapContext);
        }

        /**
        * Function to process fetched invoice details to proper key-value pair
        * @param mapContext
        */
        function processInvoiceData(mapContext){
            try{
                log.debug('Map triggered');
                let parsedValue = JSON.parse(mapContext.value);
                let customerId = parsedValue.values.entity.value;
                
                mapContext.write({
                    key: customerId,
                    value: JSON.stringify(parsedValue)
                });
            } catch(error) {
                log.error('Unexpected error occured while processing Invoice data', error.toString());
            }
        }

        /**
         * Defines the function that is executed when the reduce entry point is triggered. This entry point is triggered
         * automatically when the associated map stage is complete. This function is applied to each group in the provided context.
         * @param {Object} reduceContext - Data collection containing the groups to process in the reduce stage. This parameter is
         *     provided automatically based on the results of the map stage.
         * @param {Iterator} reduceContext.errors - Serialized errors that were thrown during previous attempts to execute the
         *     reduce function on the current group
         * @param {number} reduceContext.executionNo - Number of times the reduce function has been executed on the current group
         * @param {boolean} reduceContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {string} reduceContext.key - Key to be processed during the reduce stage
         * @param {List<String>} reduceContext.values - All values associated with a unique key that was passed to the reduce stage
         *     for processing
         * @since 2015.2
         */
        const reduce = (reduceContext) => {
            generateContentOfCSV(reduceContext);
        }

        /**
        * Function to process generate csv contents such as content, customer name, email, days overdue
        * @param reduceContext
        */
        function generateContentOfCSV(reduceContext){
            try{
                let csvContent = 'Customer Name, Customer Email, Invoice Document Number, Invoice Amount, Days Overdue \n';
                let salesRep = ''

                reduceContext.values.forEach(function(value){
                    let parsedValue = JSON.parse(value);
                    // log.debug('Parsed value in reduce', parsedValue);

                    let customerName = parsedValue.values.entity.text;
                    let customerEmail = parsedValue.values.email || 'Empty';
                    if(parsedValue.values['salesrep.customerMain'] === ''){
                        salesRep = 'Unassigned'
                    } else {
                        salesRep = parsedValue.values['salesrep.customerMain'].value;
                    }
                    let invoiceDocNumber = parsedValue.values.tranid;
                    let invoiceAmount = parsedValue.values.amount;
                    let daysOverdue = parsedValue.values.daysoverdue;
                    csvContent += `${customerName}, ${customerEmail}, ${invoiceDocNumber}, ${invoiceAmount}, ${daysOverdue}\n`
                });

                generateCustomerOverdueReport(csvContent, salesRep, reduceContext);

            } catch(error) {
                log.error('Unexpected error occured while generating content of CSV', error.toString());
            }
        }

        /**
        * Function to process generate csv file
        * @param csvContent: Content of the CSV file
        * @param salesRep: Customer sales rep
        * @param reduceContext
        */
        function generateCustomerOverdueReport(csvContent, salesRep, reduceContext){
            try{
                let customerId = reduceContext.key;
                let csvName = `Invoice Overdues of ${customerId}`;

                let csvFile = file.create({
                    name: csvName,
                    fileType: file.Type.CSV,
                    contents: csvContent,
                    description: 'The file contains invoice overdues details of customer from previous month',
                    folder: -14,
                    encoding: file.Encoding.UTF8,
                    isOnline: true
                });
                csvFile.save();

                csvContent = '';

                sendMonthlyEmail(csvFile, salesRep, customerId);
            } catch(error) {
                log.error('Unexpected error occured while generating customer overdues', error.toString());
            }
            
        }

        /**
        * Function to send email with the created csv as attachment
        * @param csvFile
        * @param salesRep
        * @param customerId
        */
        function sendMonthlyEmail(csvFile, salesRep, customerId){
            try {
                // log.debug('Sales Rep in sendMonthlyEmail function', salesRep);
                // log.debug('Customer Id in sendMonthlyEmail function', customerId);

                let salesRepDetails = true;

                if(salesRep !== 'Unassigned'){
                    salesRepDetails = search.lookupFields({
                        type: search.Type.EMPLOYEE,
                        id: salesRep,
                        columns: ['isinactive', 'email']
                    });
                }
                let isInactive = salesRepDetails.isinactive;
                log.debug('Is sales rep inactive', isInactive);
                let salesRepEmail = salesRepDetails.email || 'Email not set'
                log.debug('Sales Rep Email', salesRepEmail);

                if(salesRep === 'Unassigned' || isInactive == false || salesRepEmail === 'Email not set'){
                    log.debug('Sales Rep Unavilable');
                    email.send({
                        author: -5,
                        recipients: customerId,
                        subject: 'Reminder - Invoice Overdue',
                        body: `Hi,
                        I hope this message finds you well. This is a reminder that there are outstanding amounts on your invoice. Please find the details of the invoice below.
                        Thank you`,
                        attachments: [csvFile]
                    });
                } else {
                    log.debug('sales Rep Available');
                    email.send({
                        author: salesRep,
                        recipients: customerId,
                        subject: 'Reminder - Invoice Overdue',
                        body: `Hi, 
                        I hope this message finds you well. This is a reminder that there are outstanding amounts on your invoice. Please find the details of the invoice below.
                        Thank you`,
                        attachments: [csvFile]
                    });
                }
            } catch(error) {
                log.error('Unexpected error occured while sending email', error.toString());
            }
        }


        /**
         * Defines the function that is executed when the summarize entry point is triggered. This entry point is triggered
         * automatically when the associated reduce stage is complete. This function is applied to the entire result set.
         * @param {Object} summaryContext - Statistics about the execution of a map/reduce script
         * @param {number} summaryContext.concurrency - Maximum concurrency number when executing parallel tasks for the map/reduce
         *     script
         * @param {Date} summaryContext.dateCreated - The date and time when the map/reduce script began running
         * @param {boolean} summaryContext.isRestarted - Indicates whether the current invocation of this function is the first
         *     invocation (if true, the current invocation is not the first invocation and this function has been restarted)
         * @param {Iterator} summaryContext.output - Serialized keys and values that were saved as output during the reduce stage
         * @param {number} summaryContext.seconds - Total seconds elapsed when running the map/reduce script
         * @param {number} summaryContext.usage - Total number of governance usage units consumed when running the map/reduce
         *     script
         * @param {number} summaryContext.yields - Total number of yields when running the map/reduce script
         * @param {Object} summaryContext.inputSummary - Statistics about the input stage
         * @param {Object} summaryContext.mapSummary - Statistics about the map stage
         * @param {Object} summaryContext.reduceSummary - Statistics about the reduce stage
         * @since 2015.2
         */
        const summarize = (summaryContext) => {
            // log.debug('Email Send Successfully');
        }

        return {getInputData, map, reduce, summarize}

    });
