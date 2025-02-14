package org.sunbird.recommendation.dto;

import lombok.*;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class RecommendationDto {

    private String userId;

    private List<String> courseIds;
}
