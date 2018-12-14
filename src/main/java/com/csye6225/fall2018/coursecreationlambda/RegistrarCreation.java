package com.csye6225.fall2018.coursecreationlambda;

import java.util.Map;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

public class RegistrarCreation implements RequestHandler<Map<String, String>, String>
{

    @Override
    public String handleRequest(Map<String, String> input, Context context)
    {
        context.getLogger().log("Input: " + input);
        final Registrar registrar = new Registrar();
        registrar.setRegistrarId(input.get("courseId"));
        registrar.setOfferingId(input.get("courseId"));
        registrar.setOfferingType("Course");
        registrar.setDepartment(input.get("department"));
        registrar.setPerUnitPrice(3000);
        return postRegistrar(registrar);
    }

    private String postRegistrar(final Registrar registrar)
    {
        final Client client = ClientBuilder.newClient();
        final WebTarget webTarget = client
                .target("http://courseservice-env.9g2qwmkds6.us-west-2.elasticbeanstalk.com/webapi")
                .path("registerOffering");
        final Registrar response = webTarget.request(MediaType.APPLICATION_JSON).accept(MediaType.APPLICATION_JSON)
                .post(Entity.json(registrar), Registrar.class);
        return response != null ? response.getRegistrarId() : "";
    }
}

class Registrar
{
    private String id;
    private String registrarId;
    private String offeringId;
    private String offeringType;
    private String department;
    private Integer perUnitPrice;

    public Registrar()
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

    public String getRegistrarId()
    {
        return registrarId;
    }

    public void setRegistrarId(String registrarId)
    {
        this.registrarId = registrarId;
    }

    public String getOfferingId()
    {
        return offeringId;
    }

    public void setOfferingId(String offeringId)
    {
        this.offeringId = offeringId;
    }

    public String getOfferingType()
    {
        return offeringType;
    }

    public void setOfferingType(String offeringType)
    {
        this.offeringType = offeringType;
    }

    public String getDepartment()
    {
        return department;
    }

    public void setDepartment(String department)
    {
        this.department = department;
    }

    public Integer getPerUnitPrice()
    {
        return perUnitPrice;
    }

    public void setPerUnitPrice(Integer perUnitPrice)
    {
        this.perUnitPrice = perUnitPrice;
    }
}
