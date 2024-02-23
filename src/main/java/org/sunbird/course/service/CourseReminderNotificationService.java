package org.sunbird.course.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.sunbird.cassandra.utils.CassandraOperation;
import org.sunbird.common.util.CbExtServerProperties;
import org.sunbird.common.util.Constants;
import org.sunbird.common.util.NotificationUtil;
import org.sunbird.core.config.PropertiesConfig;
import org.sunbird.core.logger.CbExtLogger;
import org.sunbird.course.model.CourseDetails;
import org.sunbird.course.model.IncompleteCourses;
import org.sunbird.course.model.UserCourseProgressDetails;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Service
public class CourseReminderNotificationService {
	private static final CbExtLogger logger = new CbExtLogger(CourseReminderNotificationService.class.getName());
	private Map<String, CourseDetails> courseIdAndCourseNameMap = new HashMap<>();

	@Autowired
	CassandraOperation cassandraOperation;

	@Autowired
	CbExtServerProperties serverProperties;

	@Autowired
	NotificationUtil notificationUtil;

	@Autowired
	PropertiesConfig configuration;

	public void initiateCourseReminderEmail() {
		logger.info("CourseReminderNotificationService:: initiateCourseReminderEmail: Started");
		long duration = 0;
		if (serverProperties.isIncompleteCoursesAlertEnabled()) {
			long startTime = System.currentTimeMillis();
			try {
				Date date = new Date(new Date().getTime() - serverProperties.getIncompleteCoursesLastAccessTime());
				List<Map<String, Object>> userCoursesList = cassandraOperation.searchByWhereClause(
						Constants.SUNBIRD_COURSES_KEY_SPACE_NAME, Constants.USER_CONTENT_CONSUMPTION,
						Constants.COURSE_REMINDER_EMAIL_FIELDS, date);
				if (!CollectionUtils.isEmpty(userCoursesList)) {
					fetchCourseIdsAndSetCourseNameAndThumbnail(userCoursesList);
					Map<String, UserCourseProgressDetails> userCourseMap = new HashMap<>();
					setUserCourseMap(userCoursesList, userCourseMap);
					getAndSetUserEmail(userCourseMap);
					for (Map.Entry<String, UserCourseProgressDetails> userCourseProgressDetailsEntry : userCourseMap
							.entrySet()) {
						sendIncompleteCourseEmail(userCourseProgressDetailsEntry);
					}
				}
			} catch (Exception e) {
				logger.error(String.format("Error in the scheduler to send User Progress emails %s", e.getMessage()),
						e);
			}
			duration = System.currentTimeMillis() - startTime;
		}
		logger.info("CourseReminderNotificationService:: initiateCourseReminderEmail: Completed. Time taken: "
				+ duration + " milli-seconds");
	}

	private void sendIncompleteCourseEmail(
			Map.Entry<String, UserCourseProgressDetails> userCourseProgressDetailsEntry) {
		try {
			if (!StringUtils.isEmpty(userCourseProgressDetailsEntry.getValue().getEmail())
					&& userCourseProgressDetailsEntry.getValue().getIncompleteCourses().size() > 0) {
				Map<String, Object> params = new HashMap<>();
				for (int i = 0; i < userCourseProgressDetailsEntry.getValue().getIncompleteCourses().size(); i++) {
					String courseId = Constants.COURSE_KEYWORD + (i + 1);
					params.put(courseId, true);
					params.put(courseId + Constants._URL,
							userCourseProgressDetailsEntry.getValue().getIncompleteCourses().get(i).getCourseUrl());
					params.put(courseId + Constants.THUMBNAIL,
							userCourseProgressDetailsEntry.getValue().getIncompleteCourses().get(i).getThumbnail());
					params.put(courseId + Constants._NAME,
							userCourseProgressDetailsEntry.getValue().getIncompleteCourses().get(i).getCourseName());
					params.put(courseId + Constants._DURATION, String.valueOf(userCourseProgressDetailsEntry.getValue()
							.getIncompleteCourses().get(i).getCompletionPercentage()));
				}
				CompletableFuture.runAsync(() -> {
					notificationUtil.sendNotification(
							Collections.singletonList(userCourseProgressDetailsEntry.getValue().getEmail()), params,
							serverProperties.getSenderEmailAddress(),
							serverProperties.getNotificationServiceHost()
									+ serverProperties.getNotificationEventEndpoint(),
							Constants.INCOMPLETE_COURSES, Constants.INCOMPLETE_COURSES_MAIL_SUBJECT);
				});
			}
		} catch (Exception e) {
			logger.info(String.format("Error in the incomplete courses email module %s", e.getMessage()));
		}
	}

	private void fetchCourseIdsAndSetCourseNameAndThumbnail(List<Map<String, Object>> userCoursesList)
			throws IOException {
		List<String> desiredKeys = Collections.singletonList(Constants.COURSE_ID);
		Set<Object> courseIds = userCoursesList.stream()
				.flatMap(x -> desiredKeys.stream().filter(x::containsKey).distinct().map(x::get))
				.collect(Collectors.toSet());
		getAndSetCourseName(courseIds);
	}

	private void getAndSetCourseName(Set<Object> courseIds) {
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put(Constants.IDENTIFIER, courseIds.stream().collect(Collectors.toList()));
		List<Map<String, Object>> coursesDataList = cassandraOperation.getRecordsByProperties(
				configuration.getHierarchyStoreKeyspaceName(), Constants.CONTENT_HIERARCHY, propertyMap,
				Arrays.asList(Constants.IDENTIFIER, Constants.HIERARCHY));
		for (Map<String, Object> courseData : coursesDataList) {
			if (courseData.get(Constants.IDENTIFIER) != null && courseData.get(Constants.HIERARCHY) != null
					&& courseIdAndCourseNameMap.get(courseData.get(Constants.IDENTIFIER)) == null) {
				String hierarchy = (String) courseData.get(Constants.HIERARCHY);
				Map<String, Object> courseDataMap = new Gson().fromJson(hierarchy, Map.class);
				CourseDetails courseDetail = new CourseDetails();
				if (courseDataMap.get(Constants.NAME) != null) {
					courseDetail.setCourseName((String) courseDataMap.get(Constants.NAME));
				}
				if (courseDataMap.get(Constants.POSTER_IMAGE) != null) {
					courseDetail.setThumbnail((String) courseDataMap.get(Constants.POSTER_IMAGE));
				}
				courseIdAndCourseNameMap.put((String) courseData.get(Constants.IDENTIFIER), courseDetail);
			}
		}
	}

	private void getAndSetUserEmail(Map<String, UserCourseProgressDetails> userCourseMap) {
		ArrayList<String> userIds = new ArrayList<>(userCourseMap.keySet());
		Map<String, Object> propertyMap = new HashMap<>();
		propertyMap.put(Constants.ID, userIds);
		propertyMap.put(Constants.IS_DELETED, Boolean.FALSE);
		propertyMap.put(Constants.STATUS, 1);

		List<Map<String, Object>> excludeEmails = cassandraOperation.getRecordsByProperties(
				Constants.SUNBIRD_KEY_SPACE_NAME, Constants.EXCLUDE_USER_EMAILS, null,
				Collections.singletonList(Constants.EMAIL));
		List<String> desiredKeys = Collections.singletonList(Constants.EMAIL);
		List<Object> excludeEmailsList = excludeEmails.stream()
				.flatMap(x -> desiredKeys.stream().filter(x::containsKey).map(x::get)).collect(Collectors.toList());

		List<Map<String, Object>> userDetails = cassandraOperation.getRecordsByProperties(
				Constants.SUNBIRD_KEY_SPACE_NAME, Constants.TABLE_USER, propertyMap,
				Arrays.asList(Constants.ID, Constants.PROFILE_DETAILS_KEY));
		for (Map<String, Object> userDetail : userDetails) {
			try {
				if (userDetail.get(Constants.PROFILE_DETAILS_KEY) != null) {
					String profileDetails = (String) userDetail.get(Constants.PROFILE_DETAILS_KEY);
					HashMap<String, Object> profileDetailsMap = new ObjectMapper().readValue(profileDetails,
							HashMap.class);
					HashMap<String, Object> personalDetailsMap = (HashMap<String, Object>) profileDetailsMap
							.get(Constants.PERSONAL_DETAILS);
					if (MapUtils.isNotEmpty(personalDetailsMap)) {
						String email = (String) personalDetailsMap.get(Constants.PRIMARY_EMAIL);
						if (StringUtils.isNotBlank(email) && !excludeEmailsList.contains(email)) {
							userCourseMap.get(userDetail.get(Constants.ID)).setEmail(email);
						}
					}
				}
			} catch (Exception e) {
				logger.error(String.format("Error in get and set user email %s", e.getMessage()), e);
			}
		}
	}

	private void setUserCourseMap(List<Map<String, Object>> userCoursesList,
			Map<String, UserCourseProgressDetails> userCourseMap) {
		for (Map<String, Object> userCourse : userCoursesList) {
			try {
				String courseId = (String) userCourse.get(Constants.COURSE_ID);
				String batchId = (String) userCourse.get(Constants.BATCH_ID);
				String userid = (String) userCourse.get(Constants.USER_ID);
				if (courseId != null && batchId != null && courseIdAndCourseNameMap.get(courseId) != null
						&& courseIdAndCourseNameMap.get(courseId).getThumbnail() != null) {
					IncompleteCourses i = new IncompleteCourses();
					i.setCourseId(courseId);
					i.setCourseName(courseIdAndCourseNameMap.get(courseId).getCourseName());
					i.setCompletionPercentage((Float) userCourse.get(Constants.COMPLETION_PERCENTAGE));
					i.setLastAccessedDate((Date) userCourse.get(Constants.LAST_ACCESS_TIME));
					i.setBatchId(batchId);
					i.setThumbnail(courseIdAndCourseNameMap.get(courseId).getThumbnail());
					i.setCourseUrl(
							serverProperties.getCourseLinkUrl() + courseId + Constants.OVERVIEW_BATCH_KEY + batchId);
					if (userCourseMap.get(userid) != null) {
						UserCourseProgressDetails userCourseProgress = userCourseMap.get(userid);
						if (userCourseProgress.getIncompleteCourses().size() < 3) {
							userCourseProgress.getIncompleteCourses().add(i);
							userCourseProgress.getIncompleteCourses()
									.sort(Comparator.comparing(IncompleteCourses::getLastAccessedDate).reversed());
						}
					} else {
						UserCourseProgressDetails user = new UserCourseProgressDetails();
						List<IncompleteCourses> incompleteCourses = new ArrayList<>();
						incompleteCourses.add(i);
						user.setIncompleteCourses(incompleteCourses);
						userCourseMap.put(userid, user);
					}
				}
			} catch (Exception e) {
				logger.error(String.format("Error in set User Course Map %s", e.getMessage()), e);
			}
		}
	}
}