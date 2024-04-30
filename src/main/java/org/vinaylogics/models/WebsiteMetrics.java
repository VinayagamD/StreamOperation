package org.vinaylogics.models;

import java.util.HashSet;
import java.util.Set;

public class WebsiteMetrics {
    private int totalClicks;
    private int totalTimeSpent;
    private Set<String> distinctUsers;

    public WebsiteMetrics() {
        this.totalClicks = 0;
        this.totalTimeSpent = 0;
        this.distinctUsers = new HashSet<>();
    }

    public void addClick(int timeSpent, String userId) {
        totalClicks++;
        totalTimeSpent += timeSpent;
        distinctUsers.add(userId);
    }

    public void merge(WebsiteMetrics other) {
        this.totalClicks += other.totalClicks;
        this.totalTimeSpent += other.totalTimeSpent;
        this.distinctUsers.addAll(other.distinctUsers);
    }

    public int getTotalClicks() {
        return totalClicks;
    }

    public int getTotalTimeSpent() {
        return totalTimeSpent;
    }

    public int getDistinctUserCount() {
        return distinctUsers.size();
    }

    public double getAverageTimeSpent() {
        return totalClicks == 0 ? 0 : (double) totalTimeSpent / totalClicks;
    }
}
