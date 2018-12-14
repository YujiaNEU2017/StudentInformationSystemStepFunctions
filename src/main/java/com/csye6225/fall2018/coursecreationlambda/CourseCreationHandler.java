package com.csye6225.fall2018.coursecreationlambda;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent.DynamodbStreamRecord;
import com.amazonaws.services.stepfunctions.AWSStepFunctions;
import com.amazonaws.services.stepfunctions.AWSStepFunctionsClientBuilder;
import com.amazonaws.services.stepfunctions.model.StartExecutionRequest;
import com.amazonaws.services.stepfunctions.model.StartExecutionResult;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class CourseCreationHandler implements RequestHandler<DynamodbEvent, Integer>
{
    @Override
    public Integer handleRequest(DynamodbEvent event, Context context)
    {
        context.getLogger().log("Received event: " + event);

        final AWSStepFunctions stepFunctionsClient = getStepFunctionsClient();
        final StartExecutionRequest request = new StartExecutionRequest();
        request.setStateMachineArn("arn:aws:states:us-west-2:088701247392:stateMachine:courseservice-workflow");
        final ObjectMapper jsonMapper = new ObjectMapper();

        for (final DynamodbStreamRecord record : event.getRecords())
        {
            context.getLogger().log(record.getEventID());
            context.getLogger().log(record.getEventName());
            context.getLogger().log(record.getDynamodb().toString());
            if (!record.getEventName().equals("INSERT"))
            {
                continue;
            }

            final Map<String, AttributeValue> item = record.getDynamodb().getNewImage();
            if (item == null || item.get("id") == null)
            {
                continue;
            }

            try
            {
                request.setInput(jsonMapper.writeValueAsString(generateInput(item)));
                final StartExecutionResult result = stepFunctionsClient.startExecution(request);
                context.getLogger().log("Event: " + record.getEventID() + ", Result: " + result.toString());
            }
            catch (JsonProcessingException e)
            {
                context.getLogger().log("Error in generateInput, Event: " + record.getEventID());
            }
        }
        return event.getRecords().size();
    }

    private Map<String, Object> generateInput(final Map<String, AttributeValue> item)
    {
        final Map<String, Object> input = new HashMap<>();
        input.put("courseId", item.get("courseId").getS());
        input.put("department", item.get("department") != null ? item.get("department").getS() : "");
        input.put("boardId", item.get("boardId") != null ? item.get("boardId").getS() : "");
        input.put("roster", item.get("roster") != null ? item.get("roster").getSS() : "");
        return input;
    }

    private AWSStepFunctions getStepFunctionsClient()
    {
        return AWSStepFunctionsClientBuilder.standard().withRegion("us-west-2").build();
    }
}