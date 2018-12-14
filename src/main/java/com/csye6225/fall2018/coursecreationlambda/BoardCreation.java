package com.csye6225.fall2018.coursecreationlambda;

import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class BoardCreation implements RequestHandler<Map<String, String>, String>
{
    @Override
    public String handleRequest(Map<String, String> input, Context context)
    {
        context.getLogger().log("Input: " + input);
        final Board board = new Board();
        board.setBoardId(input.get("courseId"));
        board.setCourseId(input.get("courseId"));
        return postBoard(board);
    }

    private String postBoard(final Board board)
    {
        final Client client = ClientBuilder.newClient();
        final WebTarget webTarget = client
                .target("http://courseservice-env.9g2qwmkds6.us-west-2.elasticbeanstalk.com/webapi").path("boards");
        final Board response = webTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                .post(Entity.json(board), Board.class);
        return response != null ? response.getBoardId() : "";
    }
}

class Board
{
    private String id;
    private String boardId;
    private String courseId;

    public Board()
    {

    }

    public String getId()
    {
        return id;
    }

    public void setId(String id)
    {
        this.id = id;
    }

    public String getBoardId()
    {
        return boardId;
    }

    public void setBoardId(String boardId)
    {
        this.boardId = boardId;
    }

    public String getCourseId()
    {
        return courseId;
    }

    public void setCourseId(String courseId)
    {
        this.courseId = courseId;
    }
}
