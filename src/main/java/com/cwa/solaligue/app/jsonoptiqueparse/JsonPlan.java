
package com.cwa.solaligue.app.jsonoptiqueparse;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Setter
@Getter
public class JsonPlan {
    @SerializedName("containers")
    @Expose
    private List<Container> containers;

    @SerializedName("operators")
    @Expose
    private List<Operator> operators;

    @SerializedName("op_links")
    @Expose
    private List<OpLink> opLinks;
}
